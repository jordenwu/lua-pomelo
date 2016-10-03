#include "lua-pomelo.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <vector>
#include <string>
#include <mutex>
#include <unordered_map>
#include <type_traits>
#include <memory>
#include <iostream>

#include "pomelo.h"


class Action {
public:
    virtual void operator()() = 0;
    virtual ~Action() {}
};

static void push_rcstring(lua_State* L, int rc) {
    lua_pushstring(L, pc_client_rc_str(rc) + 6); // + 6 for remove the leading "PC_RC_"
}

static void push_rc_as_error(lua_State* L, int rc)
{
    if (rc == PC_RC_OK)
        lua_pushnil(L);
    else
        push_rcstring(L, rc);
}

struct LuaCallback {
    LuaCallback(): L(nullptr), ref_(LUA_NOREF) {}
    ~LuaCallback() { /* unref(); */}
    LuaCallback(lua_State* vm, int index, bool once = false) {
        set(vm, index, once);
    }

    LuaCallback(const LuaCallback& other): L(nullptr), ref_(LUA_NOREF) {
        *this = other;
    }

    LuaCallback& operator=(const LuaCallback& rhs) {
        if (this != &rhs) {
            rhs.push();
            reset(rhs.L, -1);
            lua_pop(L, 1);
        }
        return *this;
    }

    LuaCallback(LuaCallback&& other): L(nullptr), ref_(LUA_NOREF) {
        *this = std::move(other);
    }

    LuaCallback& operator=(LuaCallback&& rhs) {
        if (this != &rhs) {
            unref();

            L = rhs.L;
            ref_ = rhs.ref_;
            once_ = rhs.once_;

            rhs.L = nullptr;
            rhs.ref_ = LUA_NOREF;
        }
        return *this;
    }


    bool operator==(const LuaCallback& rhs) {
        if (L != rhs.L)
            return false;
        push();
        rhs.push();
        bool rv = lua_rawequal(L, -1, -2) != 0;
        lua_pop(L, 2);
        return rv;
    }

    bool once() const { return once_; }

    void operator()(const std::vector<std::string>& args) { // event callback
        int top = lua_gettop(L);
        push();
        for (const auto& arg : args) {
            lua_pushlstring(L, arg.data(), arg.size());
        }
        if (lua_pcall(L, args.size(), 0, 0) != 0)
            traceback(L);
        lua_settop(L, top);
    }

    void operator()(int rc, const std::string& responce) { // request callback
        int top = lua_gettop(L);
        push();
        push_rc_as_error(L, rc);
        lua_pushlstring(L, responce.data(), responce.size());
        if (lua_pcall(L, 2, 0, 0) != 0)
            traceback(L);
        lua_settop(L, top);
    }

    void operator()(int rc) { // notify callback
        int top = lua_gettop(L);
        push();
        push_rc_as_error(L, rc);
        if (lua_pcall(L, 1, 0, 0) != 0)
            traceback(L);
        lua_settop(L, top);
    }

    void push() const {
        lua_rawgeti(L, LUA_REGISTRYINDEX, ref_);
    }
private:
    lua_State* L;
    int ref_;
    bool once_;


    void unref() {
        if (L && ref_ != LUA_NOREF) {
            luaL_unref(L, LUA_REGISTRYINDEX, ref_);
            ref_ = LUA_NOREF;
        }
    }
    void reset(lua_State* aL, int index, bool once = false)
    {
        unref();
        set(aL, index, once);
    }
    void set(lua_State* vm, int index, bool once) {
        if (vm != nullptr) {
            L = vm;
            lua_pushvalue(L, index);
            ref_ = luaL_ref(L, LUA_REGISTRYINDEX);
            once_ = once;
        }
    }
    static int traceback(lua_State *L) {
        // error at -1
        if (!lua_isstring(L, -1)) {
            return 0;
        }

        lua_getglobal(L, "debug");          // [error, debug]
        if (!lua_istable(L, -1)) {
            lua_pop(L, 1);
            return 0;
        }
        lua_getfield(L, -1, "traceback");   // [error, debug, debug.traceback]

        lua_remove(L,-2);                   // [error, debug.traceback]

        lua_pushvalue(L, -2);               // [error, debug.traceback, error]
        lua_pushinteger(L, 2);              // [error, debug.traceback, error, 2]
        lua_call(L, 2, 1);                  // [error, message]
        fprintf(stderr, "%s\n", lua_tostring(L, -1));
        return 1;
    }

};

struct Signals {
    void add(std::string&& event, LuaCallback&& listener) {
        std::lock_guard<std::mutex> lock(mutex);
        events[event].push_back(listener);
    }
    void remove(const std::string& event, const LuaCallback& listener) {
        std::lock_guard<std::mutex> lock(mutex);
        auto iter = events.find(event);
        if (iter == events.end())
            return;
        std::vector<LuaCallback>& listeners = iter->second;
        auto it = std::find(listeners.begin(), listeners.end(), listener);
        if (it != listeners.cend())
            listeners.erase(it);
    }
    void clear() {
        std::lock_guard<std::mutex> lock(mutex);
        events.clear();
    }
    std::vector<LuaCallback> listeners(std::string&& event) {
        std::lock_guard<std::mutex> lock(mutex);
        return events[event];
    }
    void fire(const std::string& event, const std::vector<std::string>& args) {

        std::vector<LuaCallback> copy;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto iter = events.find(event);
            if (iter == events.end())
                return;
            auto listeners = iter->second;
            copy = listeners; // make a copy of listeners
        }

        std::cout << "fire: " << event << " to " << copy.size() << " listeners with args:" << std::endl;
        for (const auto& a : args)
            std::cout << a << std::endl;

        for (auto& h : copy) {
            h(args);
            if (h.once())
                remove(event, h);
        }
    }
    Signals() = default;
    Signals(Signals&&) = delete;
    Signals(const Signals&) = delete;
    void operator=(Signals&&) = delete;
    void operator=(const Signals&) = delete;
private:
    std::unordered_map< std::string, std::vector<LuaCallback> > events;
    std::mutex mutex;
};


static std::vector< Action* > async_events;
static std::mutex async_mutex;

pc_client_t* instance = nullptr;
pc_client_config_t config = PC_CLIENT_CONFIG_DEFAULT;
Signals events;

static void lua_event_cb(pc_client_t *client, int ev_type, void* ex_data, const char* arg1, const char* arg2);

#if LUA_VERSION_NUM < 502
#define lua_rawlen(L, idx)      lua_objlen(L, idx)
#endif

static void setfuncs(lua_State* L, const luaL_Reg *funcs)
{
#if LUA_VERSION_NUM >= 502 // LUA 5.2 or above
    luaL_setfuncs(L, funcs, 0);
#else
    luaL_register(L, NULL, funcs);
#endif
}


static int iscallable(lua_State* L, int idx)
{
    int r;
    switch (lua_type(L, idx)) {
        case LUA_TFUNCTION:
            return 1;
        case LUA_TTABLE:
            luaL_getmetafield(L, idx, "__call");
            r = lua_isfunction(L, -1);
            lua_pop(L, 1);
            return r;
        default:
            return 0;
    }
}

static const char * const log_levels[] = {
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "DISABLE",
    NULL
};


static const char* const transport_names[] = {
    "TCP", "TLS", "2", "3", "4", "5", "6", "DUMMY", NULL
};

static int streq(lua_State* L, int index, const char* v)
{
    return (lua_type(L, -1) == LUA_TSTRING
            && (strcmp(luaL_checkstring(L, -1), v) == 0));
}

static int optbool(lua_State* L, int idx, int def)
{
    return (lua_isnoneornil(L, idx)) ? def : lua_toboolean(L, idx);
}

static int recon_retry_max(lua_State* L, int idx) {
    int v;
    if (streq(L, idx, "ALWAYS"))
        return PC_ALWAYS_RETRY;
    v = (int)luaL_optinteger(L, idx, PC_ALWAYS_RETRY);
    return v;
}


/*
 * local pomelo = require('pomelo')
 * pomelo.init({
 *   log='WARN', -- log level, optional, one of 'DEBUG', 'INFO', 'WARN', 'ERROR', 'DISABLE', default to 'DISABLE'
 *   cafile = 'path/to/ca/file', -- optional
 *   capath = 'path/to/ca/path', -- optional
 *
 * })
 */
static int pomelo_init(lua_State* L)
{
    if (instance != nullptr) {
        // luaL_error(L, "pomelo.init() already called.");
        return 0;
    }

    int log_level = PC_LOG_DISABLE;
    const char* ca_file = NULL;
    const char* ca_path = NULL;

    int idx = 1;
    if (lua_istable(L, idx)) {
        lua_getfield(L, idx, "log");
        log_level = luaL_checkoption(L, -1, "DISABLE", log_levels);
        lua_pop(L, 1);

        lua_getfield(L, idx, "cafile");
        ca_file = luaL_optstring(L, -1, NULL);
        lua_pop(L, 1);

        lua_getfield(L, idx, "capath");
        ca_path = luaL_optstring(L, -1, NULL);
        lua_pop(L, 1);


        lua_getfield(L, idx, "conn_timeout");
        config.conn_timeout = (int)luaL_optinteger(L, -1, config.conn_timeout);
        lua_pop(L, 1);

        lua_getfield(L, idx, "enable_reconn");
        config.enable_reconn = optbool(L, -1, config.enable_reconn);
        lua_pop(L, 1);

        lua_getfield(L, idx, "reconn_max_retry");
        config.reconn_max_retry = recon_retry_max(L, -1);
        lua_pop(L, 1);

        lua_getfield(L, -1, "reconn_delay");
        config.reconn_delay = (int)luaL_optinteger(L, -1, config.reconn_delay);
        lua_pop(L, 1);

        lua_getfield(L, -1, "reconn_delay_max");
        config.reconn_delay_max = (int)luaL_optinteger(L, -1, config.reconn_delay_max);
        lua_pop(L, 1);

        lua_getfield(L, -1, "reconn_exp_backoff");
        config.reconn_exp_backoff = optbool(L, -1, config.reconn_exp_backoff);
        lua_pop(L, 1);

        lua_getfield(L, idx, "transport_name");
        config.transport_name = luaL_checkoption(L, -1, "TCP", transport_names);
        lua_pop(L, 1);

#if !defined(PC_NO_UV_TLS_TRANS)
        if (ca_file || ca_path) {
            tr_uv_tls_set_ca_file(ca_file, ca_path);
        }
#endif
    }

    pc_lib_set_default_log_level(log_level);

    instance = (pc_client_t*)malloc(pc_client_size());
    if (!instance)
        luaL_error(L, "error while create client instance: out of memory");
    pc_client_init(instance, nullptr, &config);
    pc_client_add_ev_handler(instance, lua_event_cb, nullptr, nullptr);

    return 0;
}

/*
* local pomelo = require('pomelo')
* pomelo.version() -- '0.3.5-release'
*/
static int pomelo_version(lua_State* L)
{
    lua_pushstring(L, pc_lib_version_str());
    return 1;
}


static int pomelo_poll(lua_State* L)
{
    (void)(L);
    std::vector< Action* > events;
    {
        std::lock_guard<std::mutex> lock(async_mutex);
        events.swap(async_events);
    }
    for (auto& p : events) {
        (*p)();
        delete p;
    }
    return 0;
}



struct arg_info_t {
    const char* event_name;
    int nargs;
    int args[2]; // args index array
};

static const struct arg_info_t ev_arg_info[PC_EV_COUNT] = {
    {        NULL, 1, {1}}, // PC_EV_USER_DEFINED_PUSH 0
    {   "connect", 0,    }, // PC_EV_CONNECTED 1
    {     "error", 1, {0}}, // PC_EV_CONNECT_ERROR 2
    {     "error", 1, {0}}, // PC_EV_CONNECT_FAILED 3
    {"disconnect", 0,    }, // PC_EV_DISCONNECT 4
    {      "kick", 0,    }, // PC_EV_KICKED_BY_SERVER 5
    {     "error", 1, {0}}, // PC_EV_UNEXPECTED_DISCONNECT 6
    {     "error", 1, {0}}, // PC_EV_PROTO_ERROR 7
};


class EventAction : public Action {
public:
    EventAction(int ev_type, const char* arg1, const char* arg2)
    {
        const char* args[] = { arg1, arg2 };
        const struct arg_info_t& info = ev_arg_info[ev_type];
        event_ = ev_type == 0 ? arg1 : info.event_name;
        for (int i = 0; i < info.nargs; ++i) {
            args_.push_back(std::string(args[info.args[i]]));
        }
    }
    virtual void operator()() override {
        events.fire(event_, args_);
    }

private:
    std::string event_;
    std::vector<std::string> args_;
};


/**
 * event handler callback and event types
 *
 * arg1 and arg2 are significant for the following events:
 *   PC_EV_USER_DEFINED_PUSH - arg1 as push route, arg2 as push msg
 *   PC_EV_CONNECT_ERROR - arg1 as short error description
 *   PC_EV_CONNECT_FAILED - arg1 as short reason description
 *   PC_EV_UNEXPECTED_DISCONNECT - arg1 as short reason description
 *   PC_EV_PROTO_ERROR - arg1 as short reason description
 *
 * For other events, arg1 and arg2 will be set to NULL.
 */
static void lua_event_cb(pc_client_t *client, int ev_type, void* ex_data, const char* arg1, const char* arg2)
{
    if (ev_type < 0 || ev_type >= PC_EV_COUNT)
        return;
    std::lock_guard<std::mutex> lock(async_mutex);
    async_events.push_back(new EventAction(ev_type, arg1, arg2));
}



static int pushRC(lua_State* L, int rc)
{
    if (rc != PC_RC_OK)
    {
        lua_pushnil(L);
        push_rcstring(L, rc);
        return 2;
    }

    lua_pushboolean(L, 1);
    return 1;
}

static int pomelo_disconnect(lua_State* L)
{
    return pushRC(L, pc_client_disconnect(instance));
}


static int pomelo_state(lua_State* L)
{
    lua_pushstring(L, pc_client_state_str(pc_client_state(instance)) + 6);
    return 1;
}


static int pomelo_conn_quality(lua_State* L)
{
    lua_pushinteger(L, pc_client_conn_quality(instance));
    return 1;
}


class RequestResponceAction : public Action {
public:
    RequestResponceAction(LuaCallback* callback, int code, const char* res) : cb_(callback), code_(code), res_(res ? res : "") { }
    virtual void operator()() override {
        (*cb_)(code_, res_);
        delete cb_;
    }
private:
    LuaCallback* cb_;
    int code_;
    std::string res_;
};

class NotifyResponceAction: public Action {
public:
    NotifyResponceAction(LuaCallback* callback, int code) : cb_(callback), code_(code) { }
    virtual void operator()() override {
        (*cb_)(code_);
        delete cb_;
    }
    LuaCallback* cb_;
    int code_;
};


struct LuaReqify { // REQuest and notIFY...
    LuaReqify() : timeout(PC_WITHOUT_TIMEOUT), callback(nullptr) {}
    int request(lua_State* L) {
        check_request(L);
        int rc = pc_request_with_timeout(instance, route, msg, callback, timeout, lua_request_cb);
        return pushRC(L, rc);
    }

    int notify(lua_State* L) {
        check_notify(L);
        int rc = pc_notify_with_timeout(instance, route, msg, callback, timeout, lua_nofity_cb);
        return pushRC(L, rc);
    }

private:
    static void lua_request_cb(const pc_request_t* req, int rc, const char* res)
    {
        LuaCallback* cb = reinterpret_cast<LuaCallback*>(pc_request_ex_data(req));
        if (!cb) return;

        std::lock_guard<std::mutex> lock(async_mutex);
        async_events.push_back(new RequestResponceAction(cb, rc, res));
    }

    static void lua_nofity_cb(const pc_notify_t* req, int rc)
    {
        LuaCallback* cb = reinterpret_cast<LuaCallback*>(pc_notify_ex_data(req));
        if (!cb) return;

        std::lock_guard<std::mutex> lock(async_mutex);
        async_events.push_back(new NotifyResponceAction(cb, rc));
    }

    static LuaCallback* createLuaCallback(lua_State* L, int index, bool error)
    {
        if (!iscallable(L, index)) {
            if (error)
                luaL_error(L, "bad argument %d (function expected, got %s)", index, luaL_typename(L, index));
            return nullptr;
        }

        return new LuaCallback(L, index, true);
    }

    int check_base(lua_State* L) {
        int nargs = lua_gettop(L);
        if (nargs > 4)
            luaL_error(L, "at most 4 arguments expected, got %d)", nargs);// never returns.

        size_t sz;
        route = luaL_checkstring(L, 1);
        msg = luaL_checklstring(L, 2, &sz);
        if (sz == 0)
            luaL_argerror(L, 3, "message should not be empty");
        return nargs;
    }

    void check_request(lua_State* L)
    {
        int nargs = check_base(L);
        if (nargs < 3)
            luaL_error(L, "3 or 4 arguments expected, got %d)", nargs);

        callback = createLuaCallback(L, nargs, true);

        if (nargs == 4) { // then 3rd arg is timeout
            timeout = luaL_checkinteger(L, 3);
        }
    }

    void check_notify(lua_State* L)
    {
        int nargs = check_base(L);
        switch (nargs) {
            case 3:
                callback = createLuaCallback(L, 3, 0);
                if (!callback)
                    timeout = luaL_checkinteger(L, 3);
                break;
            case 4:
                callback = createLuaCallback(L, 4, 0);
                //if (!callback)
                timeout = luaL_checkinteger(L, 3);
                break;
            case 5:
                timeout = luaL_checkinteger(L, 4);
                callback = createLuaCallback(L, 5, true);
                break;
            default:
                break;
        }
    }

    const char* route;
    const char* msg;
    int timeout;
    LuaCallback* callback;
};


/**
 * client:request(route, message[, timeout], callback)
 * client:request('connector.get.ip', message, 30, function(err, req, res)
 *
 * end)
 */
static int pomelo_request(lua_State* L)
{
    return LuaReqify().request(L);
}


/**
 * client:notify(route, message[, timeout][, callback])
 * client:notify('connector.get.ip', message, 30, function(err, req)
 *
 * end)
 */
static int pomelo_notify(lua_State* L)
{
    return LuaReqify().notify(L);
}


static int _on(lua_State* L, bool once) {
    // [event, callback]
    size_t sz;
    const char* event = luaL_checklstring(L, 1, &sz);
    luaL_argcheck(L, iscallable(L, 2), 2, "should be a callback");

    events.add(std::string(event, sz), LuaCallback(L, 2, once));

    return 0;
}

/**
 * client:on(route, callback) --> client
 */
static int pomelo_on(lua_State* L)
{
    return _on(L, false);
}


// client:once(route, callback) --> client
static int pomelo_once(lua_State* L)
{
    return _on(L, true);
}

static int pomelo_off(lua_State* L)
{
    size_t sz;
    const char* event = luaL_checklstring(L, 1, &sz);
    luaL_argcheck(L, iscallable(L, 2), 2, "should be a callback");

    events.remove(std::string(event, sz), LuaCallback(L, 2));

    return 0;
}

// pomelo.listeners(event)
// Returns a copy of the array of listeners for the specified event.
static int pomelo_listeners(lua_State* L)
{
    size_t sz;
    const char* event = luaL_checklstring(L, 1, &sz);
    auto listeners = events.listeners(std::string(event, sz));

    lua_createtable(L, listeners.size(), 0);        // [event, listeners]
    for (int i = 0; i < listeners.size(); ++i) {
        listeners[i].push();                        // [event, listeners, listener]
        lua_rawseti(L, -2, i+1);                    // [event, listeners]
    }
    return 1;
}

static int pomelo_removeAllListeners(lua_State* L)
{
    events.clear();
    return 0;
}


static int pomelo_connect(lua_State* L)
{
    const char* host = luaL_checkstring(L, 1);
    int port = (int)luaL_checkinteger(L, 2);
    const char* handshake_opts = luaL_optstring(L, 3, NULL);
    return pushRC(L, pc_client_connect(instance, host, port, handshake_opts));
}


static const luaL_Reg lib[] = {
    {"init", pomelo_init},
    {"version", pomelo_version},

    {"connect", pomelo_connect},
    {"disconnect", pomelo_disconnect},
    {"request", pomelo_request},
    {"notify", pomelo_notify},
    {"poll", pomelo_poll},

    {"on", pomelo_on},
    {"addListener", pomelo_on},     // Alias for on
    {"once", pomelo_once},
    {"off", pomelo_off},
    {"removeListener", pomelo_off}, // Alias for off
    {"listeners", pomelo_listeners},
    {"removeAllListeners", pomelo_removeAllListeners},

    {"state", pomelo_state},
    {"conn_quality", pomelo_conn_quality},
    {"connQuality", pomelo_conn_quality},   // Alias for conn_quality

    {NULL, NULL}
};


static int initialized = 0;
LUALIB_API int luaopen_pomelo(lua_State *L)
{
    if (!initialized) {
        initialized = 1;
        pc_lib_set_default_log_level(PC_LOG_DISABLE);
        pc_lib_init(NULL, NULL, NULL, "Lua Client");
    }

    lua_createtable(L, 0, sizeof(lib)/sizeof(lib[0])-1);
    setfuncs(L, lib);
    return 1;
}
