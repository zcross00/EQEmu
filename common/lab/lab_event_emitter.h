#ifndef EQEMU_LAB_EVENT_EMITTER_H
#define EQEMU_LAB_EVENT_EMITTER_H

#include <functional>
#include <string>

// Observable Lab — structured event emitter + control back-channel (lab fork).
//
// Pushes line-delimited JSON `lab-schema` events to the Rust gateway's TCP
// ingest. The hot path (EmitLogLine) is non-blocking: it serializes the event
// and enqueues it on a bounded queue (drop-oldest under backpressure) so it can
// never stall the 32ms zone tick. A background thread owns the socket and
// reconnects on failure. The same socket is read for control commands the
// gateway pushes back (e.g. set_log_level) — so no new listening port is needed
// on the container side. Entirely dormant unless EmitterStart is called.
namespace lab {

// Set the handler invoked when the gateway pushes a control command back over
// the emitter socket. Call this BEFORE EmitterStart so it is visible to the
// reader thread without a race.
void EmitterSetControlHandler(std::function<void(const std::string &verb, const std::string &args)> handler);

// Drain queued control commands, running the registered handler for each. Call
// once per main zone tick so handlers execute on the main thread (entity-safe).
void EmitterProcessControls();

// Start the background emitter for this process. `addr` is "host:port"; if
// empty, falls back to $LAB_EMIT_ADDR, then 127.0.0.1:7701. Idempotent.
void EmitterStart(const std::string &source_driver,
                  const std::string &zone_short_name,
                  int instance_id,
                  const std::string &addr = "");

// Enqueue a structured log line as a lab-schema LogLine event. Non-blocking;
// a no-op if the emitter was never started.
void EmitLogLine(const char *category, const char *level, const std::string &text);

// Typed semantic events (lab-schema EventKind variants) emitted at the source.
// Non-blocking; no-ops if the emitter was never started.
void EmitDeath(const char *victim, const char *killer, int victim_level, float x, float y, float z);
void EmitLoot(const char *looter, const char *corpse_of, unsigned int item_id, const char *item_name, float x, float y, float z);
void EmitSpawn(const char *name, bool npc, int level, float x, float y, float z);
void EmitHpUpdate(const char *name, int hp_pct, float x, float y, float z);

// Stop and join the worker (process shutdown). Safe if never started.
void EmitterStop();

} // namespace lab

#endif // EQEMU_LAB_EVENT_EMITTER_H
