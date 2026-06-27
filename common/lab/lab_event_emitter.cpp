#include "lab_event_emitter.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#ifdef __unix__
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace lab {
namespace {

// Drop the oldest queued event past this depth — bounding memory and
// guaranteeing the hot path never blocks on a slow/absent gateway.
constexpr size_t kMaxQueue = 8192;

// Suppress Spawn events for this long after EmitterStart. The zone populates
// its entire spawn2 roster at boot (a flood the peq oracle already covers), so
// only spawns *after* the zone has settled — repops, roamers, summons, pets —
// are the novel deltas worth streaming to the world-model.
constexpr int64_t kSpawnSettleMs = 30000;

struct Emitter {
	// Default to the Docker-Desktop host alias so zones reach the Windows-host
	// gateway without any LAB_EMIT_ADDR env (resolved via getaddrinfo). Override
	// with $LAB_EMIT_ADDR (host:port) for other topologies.
	std::string host = "host.docker.internal";
	int         port = 7701;
	std::string source_prefix; // precomputed `"source":{...}` fragment
	int64_t     start_ms = 0;  // EmitterStart wall-clock — gates the spawn settle window

	std::mutex              mtx;
	std::condition_variable cv;
	std::deque<std::string> queue;
	std::atomic<bool>       running{false};
	std::thread             worker;

	// Control back-channel: gateway -> server.
	std::function<void(const std::string &, int)> control_handler;

	void run();
	int  connect_socket();
	void read_control(int fd);
	void handle_control_line(const std::string &line);
};

Emitter g_emitter;

std::string json_escape(const std::string &s)
{
	std::string out;
	out.reserve(s.size() + 8);
	for (char c : s) {
		switch (c) {
			case '"': out += "\\\""; break;
			case '\\': out += "\\\\"; break;
			case '\n': out += "\\n"; break;
			case '\r': out += "\\r"; break;
			case '\t': out += "\\t"; break;
			default:
				if (static_cast<unsigned char>(c) < 0x20) {
					char buf[8];
					std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
					out += buf;
				}
				else {
					out += c;
				}
		}
	}
	return out;
}

int64_t now_ms()
{
	using namespace std::chrono;
	return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string vec3_json(float x, float y, float z)
{
	return "{\"x\":" + std::to_string(x) + ",\"y\":" + std::to_string(y) + ",\"z\":" + std::to_string(z) + "}";
}

// Wrap a kind object with ts + source and enqueue it (non-blocking, drop-oldest).
void enqueue_event(const std::string &kind_json)
{
	if (!g_emitter.running.load()) {
		return;
	}
	std::string line = "{\"ts\":" + std::to_string(now_ms()) + ",";
	line += g_emitter.source_prefix;
	line += ",\"kind\":";
	line += kind_json;
	line += "}";
	{
		std::lock_guard<std::mutex> lk(g_emitter.mtx);
		if (g_emitter.queue.size() >= kMaxQueue) {
			g_emitter.queue.pop_front();
		}
		g_emitter.queue.push_back(std::move(line));
	}
	g_emitter.cv.notify_one();
}

int Emitter::connect_socket()
{
#ifdef __unix__
	// Resolve host (hostname like host.docker.internal, or a numeric IP) via
	// getaddrinfo so the emitter works without a hard-coded host IP.
	addrinfo  hints{};
	addrinfo *res = nullptr;
	hints.ai_family   = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	const std::string port_str = std::to_string(port);
	if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
		return -1;
	}
	int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (fd < 0) {
		::freeaddrinfo(res);
		return -1;
	}
	if (::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
		::close(fd);
		::freeaddrinfo(res);
		return -1;
	}
	::freeaddrinfo(res);
	return fd;
#else
	return -1;
#endif
}

// Apply one control line, e.g. "set_log_level Combat 0".
void Emitter::handle_control_line(const std::string &line)
{
	// Wire form: "set_log_level <level> <category…>" — level first so a
	// category with spaces (e.g. "Quest Debug") is the rest of the line.
	std::istringstream iss(line);
	std::string        verb;
	int                level = 0;
	iss >> verb >> level;
	std::string category;
	std::getline(iss, category);
	const size_t start = category.find_first_not_of(" \t");
	category = (start == std::string::npos) ? std::string() : category.substr(start);
	if (verb == "set_log_level" && !category.empty() && control_handler) {
		control_handler(category, level);
	}
}

void Emitter::read_control(int fd)
{
#ifdef __unix__
	std::string buf;
	char        tmp[512];
	while (running.load()) {
		ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
		if (n <= 0) {
			break; // socket closed/shutdown
		}
		buf.append(tmp, static_cast<size_t>(n));
		size_t pos;
		while ((pos = buf.find('\n')) != std::string::npos) {
			handle_control_line(buf.substr(0, pos));
			buf.erase(0, pos + 1);
		}
	}
#endif
}

void Emitter::run()
{
#ifdef __unix__
	while (running.load()) {
		int fd = connect_socket();
		if (fd < 0) {
			std::this_thread::sleep_for(std::chrono::seconds(2));
			continue;
		}
		// Concurrent reader for the control back-channel (read+write on one
		// socket fd is safe). It unblocks via shutdown() below on disconnect.
		std::thread reader([this, fd] { read_control(fd); });

		while (running.load()) {
			std::string line;
			{
				std::unique_lock<std::mutex> lk(mtx);
				cv.wait(lk, [&] { return !queue.empty() || !running.load(); });
				if (!running.load()) {
					break;
				}
				line = std::move(queue.front());
				queue.pop_front();
			}
			line.push_back('\n');
			ssize_t off = 0;
			bool    ok  = true;
			while (off < static_cast<ssize_t>(line.size())) {
				ssize_t n = ::send(fd, line.data() + off, line.size() - off, MSG_NOSIGNAL);
				if (n <= 0) {
					ok = false;
					break;
				}
				off += n;
			}
			if (!ok) {
				break; // socket died — reconnect
			}
		}

		::shutdown(fd, SHUT_RDWR); // unblock the reader's recv
		::close(fd);
		if (reader.joinable()) {
			reader.join();
		}
	}
#endif
}

} // namespace

void EmitterSetControlHandler(std::function<void(const std::string &category, int level)> handler)
{
	g_emitter.control_handler = std::move(handler);
}

void EmitterStart(
	const std::string &source_driver,
	const std::string &zone_short_name,
	int                instance_id,
	const std::string &addr)
{
	bool expected = false;
	if (!g_emitter.running.compare_exchange_strong(expected, true)) {
		return; // already started
	}
	g_emitter.start_ms = now_ms();

	std::string a = addr;
	if (a.empty()) {
		const char *env = std::getenv("LAB_EMIT_ADDR");
		if (env && *env) {
			a = env;
		}
	}
	if (!a.empty()) {
		auto colon = a.find(':');
		if (colon != std::string::npos) {
			g_emitter.host = a.substr(0, colon);
			g_emitter.port = std::atoi(a.c_str() + colon + 1);
		}
	}

	std::string src = "\"source\":{\"driver\":\"" + json_escape(source_driver) + "\"";
	if (!zone_short_name.empty()) {
		src += ",\"zone\":\"" + json_escape(zone_short_name) + "\"";
	}
	src += ",\"instance\":" + std::to_string(instance_id) + "}";
	g_emitter.source_prefix = src;

	g_emitter.worker = std::thread([] { g_emitter.run(); });
}

void EmitLogLine(const char *category, const char *level, const std::string &text)
{
	if (!g_emitter.running.load()) {
		return;
	}
	std::string line = "{\"ts\":" + std::to_string(now_ms()) + ",";
	line += g_emitter.source_prefix;
	line += ",\"kind\":{\"type\":\"log_line\",\"category\":\"";
	line += json_escape(category ? category : "");
	line += "\",\"level\":\"";
	line += json_escape(level ? level : "Info");
	line += "\",\"text\":\"";
	line += json_escape(text);
	line += "\"}}";

	{
		std::lock_guard<std::mutex> lk(g_emitter.mtx);
		if (g_emitter.queue.size() >= kMaxQueue) {
			g_emitter.queue.pop_front();
		}
		g_emitter.queue.push_back(std::move(line));
	}
	g_emitter.cv.notify_one();
}

void EmitDeath(const char *victim, const char *killer, int victim_level, float x, float y, float z)
{
	std::string kind = "{\"type\":\"death\",\"victim\":\"";
	kind += json_escape(victim ? victim : "");
	kind += "\",\"killer\":\"";
	kind += json_escape(killer ? killer : "");
	kind += "\",\"victim_level\":" + std::to_string(victim_level);
	kind += ",\"at\":" + vec3_json(x, y, z) + "}";
	enqueue_event(kind);
}

void EmitLoot(const char *looter, const char *corpse_of, unsigned int item_id, const char *item_name, float x, float y, float z)
{
	std::string kind = "{\"type\":\"loot\",\"looter\":\"";
	kind += json_escape(looter ? looter : "");
	kind += "\",\"corpse_of\":\"";
	kind += json_escape(corpse_of ? corpse_of : "");
	kind += "\",\"item_id\":" + std::to_string(item_id);
	kind += ",\"item_name\":\"";
	kind += json_escape(item_name ? item_name : "");
	kind += "\",\"at\":" + vec3_json(x, y, z) + "}";
	enqueue_event(kind);
}

void EmitSpawn(const char *name, bool npc, int level, float x, float y, float z)
{
	// Skip the zone-boot population flood; only stream post-settle spawns.
	if (!g_emitter.running.load() || (now_ms() - g_emitter.start_ms) < kSpawnSettleMs) {
		return;
	}
	std::string kind = "{\"type\":\"spawn\",\"name\":\"";
	kind += json_escape(name ? name : "");
	kind += "\",\"npc\":";
	kind += (npc ? "true" : "false");
	kind += ",\"level\":" + std::to_string(level);
	kind += ",\"at\":" + vec3_json(x, y, z) + "}";
	enqueue_event(kind);
}

void EmitHpUpdate(const char *name, int hp_pct, float x, float y, float z)
{
	std::string kind = "{\"type\":\"hp_update\",\"name\":\"";
	kind += json_escape(name ? name : "");
	kind += "\",\"hp_pct\":" + std::to_string(hp_pct);
	kind += ",\"at\":" + vec3_json(x, y, z) + "}";
	enqueue_event(kind);
}

void EmitterStop()
{
	if (!g_emitter.running.exchange(false)) {
		return;
	}
	g_emitter.cv.notify_all();
	if (g_emitter.worker.joinable()) {
		g_emitter.worker.join();
	}
}

} // namespace lab
