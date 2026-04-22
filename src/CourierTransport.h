#ifndef COURIER_TRANSPORT_H
#define COURIER_TRANSPORT_H

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <functional>
#include <atomic>

class CourierTransport {
public:
    using MessageCallback = std::function<void(const char* payload, size_t length)>;
    using BinaryMessageCallback = std::function<void(const uint8_t* data, size_t length)>;
    using ConnectionCallback = std::function<void(CourierTransport* transport, bool connected)>;

    virtual ~CourierTransport() {
        free(_pendingPayload);
        free(_pendingBinary);
    }

    virtual void begin(const char* host, uint16_t port, const char* path) = 0;
    virtual void disconnect() = 0;
    virtual void loop() { drainPending(); }
    virtual bool isConnected() const = 0;
    virtual bool send(const char* payload) = 0;
    virtual bool sendBinary(const uint8_t* data, size_t len) { (void)data; (void)len; return false; }
    virtual bool publish(const char* topic, const char* payload) {
        (void)topic;
        return send(payload);  // default: ignore topic, just send
    }
    virtual bool topicRequired() const { return false; }
    virtual const char* name() const = 0;

    // Suspend/resume: stops the transport task to free its stack memory
    // (e.g. before OTA TLS handshake), then restarts without full teardown.
    virtual void suspend() {}
    virtual void resume() {}

    virtual bool isPersistent() const { return true; }

    using FailureCallback = std::function<void()>;
    void setFailureCallback(FailureCallback cb) { _onFailure = cb; }

    void setMessageCallback(MessageCallback cb) { _onMessage = cb; }
    void setBinaryMessageCallback(BinaryMessageCallback cb) { _onBinaryMessage = cb; }
    void setConnectionCallback(ConnectionCallback cb) { _onConnection = cb; }

protected:
    MessageCallback _onMessage;
    BinaryMessageCallback _onBinaryMessage;
    ConnectionCallback _onConnection;
    FailureCallback _onFailure;

    // --- Cross-task pending message buffer ---
    // Transport task writes via queueIncomingMessage/queueConnectionChange.
    // Main loop drains via drainPending() (called from loop()).

    char* _pendingPayload = nullptr;
    size_t _pendingLength = 0;
    std::atomic<bool> _msgPending{false};

    uint8_t* _pendingBinary = nullptr;
    size_t _pendingBinaryLength = 0;
    std::atomic<bool> _binaryPending{false};

    std::atomic<bool> _connChangePending{false};
    std::atomic<bool> _connChangeState{false};
    std::atomic<bool> _failurePending{false};

    // NOTE: Single-slot pending buffer. If the main loop doesn't call loop()
    // fast enough, messages arriving while a previous message is pending will
    // be silently dropped. This is a deliberate trade-off for minimal RAM
    // usage on ESP32. For high-throughput use cases, consider replacing with
    // a FreeRTOS queue or ring buffer.

    // Called from transport event handler (may be on a different task).
    // Copies payload into a malloc'd buffer and sets the pending flag.
    void queueIncomingMessage(const char* payload, size_t len) {
        if (_msgPending.load(std::memory_order_acquire)) {
            // Previous message still pending — drop this one
            return;
        }
        char* buf = (char*)malloc(len + 1);
        if (!buf) return;
        memcpy(buf, payload, len);
        buf[len] = '\0';
        _pendingPayload = buf;
        _pendingLength = len;
        _msgPending.store(true, std::memory_order_release);
    }

    // Binary equivalent. Separate pending slot so a high-rate binary stream
    // (e.g. audio) does not contend with the text channel.
    void queueIncomingBinary(const uint8_t* data, size_t len) {
        if (_binaryPending.load(std::memory_order_acquire)) {
            return;
        }
        uint8_t* buf = (uint8_t*)malloc(len);
        if (!buf) return;
        memcpy(buf, data, len);
        _pendingBinary = buf;
        _pendingBinaryLength = len;
        _binaryPending.store(true, std::memory_order_release);
    }

    // Called from transport event handler.
    void queueConnectionChange(bool connected) {
        _connChangeState.store(connected, std::memory_order_relaxed);
        _connChangePending.store(true, std::memory_order_release);
    }

    void queueTransportFailed() {
        _failurePending.store(true, std::memory_order_release);
    }

    // Called from loop() on the main task. Fires callbacks if pending.
    void drainPending() {
        if (_msgPending.load(std::memory_order_acquire)) {
            if (_onMessage) _onMessage(_pendingPayload, _pendingLength);
            free(_pendingPayload);
            _pendingPayload = nullptr;
            _msgPending.store(false, std::memory_order_release);
        }
        if (_binaryPending.load(std::memory_order_acquire)) {
            if (_onBinaryMessage) _onBinaryMessage(_pendingBinary, _pendingBinaryLength);
            free(_pendingBinary);
            _pendingBinary = nullptr;
            _binaryPending.store(false, std::memory_order_release);
        }
        if (_connChangePending.load(std::memory_order_acquire)) {
            bool state = _connChangeState.load(std::memory_order_relaxed);
            _connChangePending.store(false, std::memory_order_release);
            if (_onConnection) _onConnection(this, state);
        }
        if (_failurePending.load(std::memory_order_acquire)) {
            _failurePending.store(false, std::memory_order_release);
            if (_onFailure) _onFailure();
        }
    }
};

#endif // COURIER_TRANSPORT_H
