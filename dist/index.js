"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const redis_1 = require("redis");
const node_crypto_1 = require("node:crypto");
const lru_cache_1 = require("lru-cache");
/** RedisSessions

 To create a new instance use:

    RedisSessions = require("redis-sessions")
    rs = new RedisSessions()

    Parameters:

    `port`: *optional* Default: `6379`. The Redis port.
    `host`, *optional* Default: `127.0.0.1`. The Redis host.
    `options`, *optional* Default: `{}`. Additional options. See [https://github.com/mranney/node_redis#rediscreateclientport-host-options](redis.createClient))
    `namespace`: *optional* Default: `rs`. The namespace prefix for all Redis keys used by this module.
    `wipe`: *optional* Default: `600`. The interval in second after which the timed out sessions are wiped. No value less than 10 allowed.
    `cachetime` (Number) *optional* Default: `0`. Number of seconds to cache sessions in memory.
    `cachemax` (Number) *optional* Default: `5000`. Maximum number of sessions stored in the cache.
*/
/**
 *
 *
 * @class RedisSessions
 *
 * @template SessionData
 *
 * @param port
 */
class RedisSessions {
    // redis name space
    redisns;
    // to check if the cache is enabled
    isCache = false;
    // redis client
    redis;
    // lru cache to store sessions
    sessionCache = null;
    // deletes sessions from redis based on ttl
    wiperInterval = null;
    // redissub is used to wipe cache on set/kill
    redissub = null;
    // handles async work of connecting to redis
    subscribed = false;
    toSubscribe = Promise.resolve(true);
    // handles async work of connecting redissub and subscribing to channel
    connected;
    toConnect;
    constructor(redisSessionsOptions) {
        redisSessionsOptions = redisSessionsOptions || {};
        this.redisns = redisSessionsOptions.namespace ?? "rs";
        this.redisns += ":";
        if (redisSessionsOptions.options && redisSessionsOptions.options.url) {
            this.redis = (0, redis_1.createClient)(redisSessionsOptions.options);
        }
        else {
            this.redis = (0, redis_1.createClient)(lodash_1.default.merge(redisSessionsOptions.options ?? {}, { socket: { port: redisSessionsOptions.port ?? 6379, host: redisSessionsOptions.host ?? "127.0.0.1" } }));
        }
        this.connected = false;
        // to handle async connect of client
        this.toConnect = this.connect();
        if (redisSessionsOptions.cachetime && redisSessionsOptions.cachetime > 0) {
            // Setup lru-cache
            this.sessionCache = new lru_cache_1.LRUCache({
                max: redisSessionsOptions.cachemax ? (redisSessionsOptions.cachemax > 0 ? redisSessionsOptions.cachemax : 5000) : 5000,
                ttl: redisSessionsOptions.cachetime * 1000,
                updateAgeOnGet: false,
                ttlAutopurge: false
            });
            // Setup the Redis subscriber to listen for changes
            if (redisSessionsOptions.options && redisSessionsOptions.options.url) {
                this.redissub = (0, redis_1.createClient)(redisSessionsOptions.options);
            }
            else {
                this.redissub = (0, redis_1.createClient)(lodash_1.default.merge(redisSessionsOptions.options ?? {}, { socket: { port: redisSessionsOptions.port ?? 6379, host: redisSessionsOptions.host ?? "127.0.0.1" } }));
            }
            // Setup the subscriber
            this.isCache = true;
            // Setup the subscriber
            this.toSubscribe = this.subscribe();
        }
        if (redisSessionsOptions.wipe !== 0) {
            let wipe = redisSessionsOptions.wipe || 600;
            if (wipe < 10) {
                wipe = 10;
            }
            this.wiperInterval = setInterval(this._wipe, wipe * 1000);
        }
    }
    /* Activity

    Get the number of active unique users (not sessions!) within the last *n* seconds

    **Parameters:**

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    * `deltaTime` Delta time. Amount of seconds to check (e.g. 600 for the last 10 min.)
    */
    async activity(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "deltaTime"]);
        const count = await this.redis.zCount(`${this.redisns}${options.app}:_users`, this._now() - options.deltaTime, "+inf");
        return { activity: count };
    }
    /* Create

    Creates a session for an app and id.

    **Parameters:**

    An object with the following keys:

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    * `id` must be [a-zA-Z0-9_-] and 1-64 chars long
    * `ip` must be a valid IP4 address
    * `ttl` *optional* Default: 7200. Positive integer between 1 and 2592000 (30 days)

    * `d` *optional* Default: undefined. Object containing additional information. Only string, number, boolean or null values
    * `no_resave` *optional* Default: false. Boolean if true ttl will not refresh

    **Example:**

        create({
            app: "forum",
            id: "user1234",
            ip: "156.78.90.12",
            ttl: 3600
        }, callback)

    Returns the token when successful.
    */
    async create(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        options.d = options.d || { ___duMmYkEy: null };
        this._validate(options, [
            "app",
            "id",
            "ip",
            "ttl",
            "d",
            "no_resave"
        ]);
        const token = this._createToken();
        // Prepopulate the multi statement
        const mc = this._createMultiStatement(options.app, token, options.id, options.ttl ?? 7200, false);
        mc.sAdd(`${this.redisns}${options.app}:us:${options.id}`, token);
        // Create the default session hash
        const thesession = {
            id: options.id,
            r: 1,
            w: 1,
            ip: options.ip,
            la: this._now(),
            ttl: options.ttl ?? 7200
        };
        if (!options.d.___duMmYkEy) {
            // Remove null values
            const nullkeys = [];
            for (const e of Object.keys(options.d)) {
                if (options.d[e] === null) {
                    nullkeys.push(e);
                }
            }
            options.d = lodash_1.default.omit(options.d, nullkeys);
            if (lodash_1.default.keys(options.d).length > 0) {
                thesession.d = JSON.stringify(options.d);
            }
        }
        // Check for `no_resave` #36
        if (options.no_resave) {
            thesession.no_resave = 1;
        }
        mc.hSet(`${this.redisns}${options.app}:${token}`, thesession);
        // Run the redis statement
        const resp = await mc.exec();
        // curently returns number of insertet key value pairs
        // old:resp[4] !== "OK"
        if (typeof resp[4] !== "number" || resp[4] < 4) {
            throw new Error("Unknown Error");
        }
        return { token: token };
    }
    /* Get

    Get a session for an app and token.

    **Parameters:**

    An object with the following keys:

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    * `token` must be [a-zA-Z0-9] and 64 chars long

    * _noupdate & nocache used by kill/set functions to skip certain parts in this function
    * if _noupdate is set session wont be cached

    important : When not supplying a d property in create and only partially setting it via the set function,
    be aware that get can return a Session with a defined d property that is missing properties from the type
    */
    async get(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "token"]);
        const cachekey = `${options.app}:${options.token}`;
        if (this.isCache && !options._nocache && this.sessionCache) {
            // Try to find the session in cache
            const cache = this.sessionCache.get(cachekey);
            if (cache) {
                return cache;
            }
        }
        const thekey = `${this.redisns}${cachekey}`;
        const resp = await this.redis.hmGet(thekey, [
            "id",
            "r",
            "w",
            "ttl",
            "d",
            "la",
            "ip",
            "no_resave"
        ]);
        const o = this._prepareSession(resp, options.token);
        if (o === null) {
            return null;
        }
        // Secret switch to disable updating the stats - we don't need this when we kill a session
        if (options._noupdate) {
            return o;
        }
        if (this.isCache && this.sessionCache) {
            this.sessionCache.set(cachekey, o);
        }
        // Update the counters
        const mc = this._createMultiStatement(options.app, options.token, o.id, o.ttl, o.no_resave);
        mc.hIncrBy(thekey, "r", 1);
        if (o.idle > 1) {
            mc.hSet(thekey, "la", this._now());
        }
        await mc.exec();
        return o;
    }
    /* Kill

    Kill a session for an app and token.

    **Parameters:**

    An object with the following keys:

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    * `token` must be [a-zA-Z0-9] and 64 chars long
    */
    async kill(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "token"]);
        const getOptions = {
            app: options.app,
            token: options.token,
            _nouupdate: true
        };
        const resp = await this.get(getOptions);
        if (!resp) {
            return { kill: 0 };
        }
        const killOptions = {
            id: resp.id,
            app: options.app,
            token: options.token
        };
        return await this._kill(killOptions);
    }
    /* Helper to _kill a single session

    Used by @kill and @wipe

    Needs options.app, options.token and options.id
    */
    async _kill(options) {
        const mc = this.redis.multi();
        mc.zRem(`${this.redisns}${options.app}:_sessions`, `${options.token}:${options.id}`);
        mc.sRem(`${this.redisns}${options.app}:us:${options.id}`, `${options.token}`);
        mc.zRem(`${this.redisns}SESSIONS`, `${options.app}:${options.token}:${options.id}`);
        mc.del(`${this.redisns}${options.app}:${options.token}`);
        mc.exists(`${this.redisns}${options.app}:us:${options.id}`);
        if (this.isCache) {
            if (!this.subscribed) {
                this.subscribed = await this.toSubscribe;
            }
            mc.publish(`${this.redisns}cache`, `${options.app}:${options.token}`);
        }
        const resp = await mc.exec();
        if (resp[4] === 0) {
            await this.redis.zRem(`${this.redisns}${options.app}:_users`, `${options.id}`);
        }
        return { kill: resp[3] };
    }
    /* Killall

    Kill all sessions of a single app

    Parameters:

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    */
    async killall(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app"]);
        // First we need to get all sessions of the app
        const appsessionkey = `${this.redisns}${options.app}:_sessions`;
        const appuserkey = `${this.redisns}${options.app}:_users`;
        const resp = await this.redis.zRange(appsessionkey, 0, -1);
        if (resp.length === 0) {
            return { kill: 0 };
        }
        const globalkeys = [];
        const tokenkeys = [];
        let userkeys = [];
        for (const e of resp) {
            const thekey = e.split(":");
            globalkeys.push(`${options.app}:${e}`);
            tokenkeys.push(`${this.redisns}${options.app}:${thekey[0]}`);
            userkeys.push(thekey[1]);
        }
        userkeys = lodash_1.default.uniq(userkeys);
        const ussets = [];
        for (const e of userkeys) {
            ussets.push(`${this.redisns}${options.app}:us:${e}`);
        }
        const mc = this.redis.multi();
        mc.zRem(appsessionkey, resp);
        mc.zRem(appuserkey, userkeys);
        mc.zRem(`${this.redisns}SESSIONS`, globalkeys);
        mc.del(ussets);
        mc.del(tokenkeys);
        if (this.isCache) {
            if (!this.subscribed) {
                this.subscribed = await this.toSubscribe;
            }
            for (const e of resp) {
                mc.publish(`${this.redisns}cache`, `${options.app}:${e.split(":")[0]}`);
            }
        }
        const response = await mc.exec();
        return { kill: response[0] };
    }
    /* Kill all Sessions of Id

    Kill all sessions of a single id within an app

    Parameters:

    * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
    * `id` must be [a-zA-Z0-9_-] and 1-64 chars long
    */
    async killsoid(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "id"]);
        const resp = await this.redis.sMembers(`${this.redisns}${options.app}:us:${options.id}`);
        if (resp.length === 0) {
            return { kill: 0 };
        }
        const mc = this.redis.multi();
        if (this.isCache && !this.subscribed) {
            this.subscribed = await this.toSubscribe;
        }
        // Grab all sessions we need to get
        for (const token of resp) {
            // Add the multi commands
            mc.zRem(`${this.redisns}${options.app}:_sessions`, `${token}:${options.id}`);
            mc.sRem(`${this.redisns}${options.app}:us:${options.id}`, token);
            mc.zRem(`${this.redisns}SESSIONS`, `${options.app}:${token}:${options.id}`);
            mc.del(`${this.redisns}${options.app}:${token}`);
            if (this.isCache) {
                mc.publish(`${this.redisns}cache`, `${options.app}:${token}`);
            }
        }
        mc.exists(`${this.redisns}${options.app}:us:${options.id}`);
        const response = await mc.exec();
        // get the amount of deleted sessions
        let total = 0;
        const ref = response.slice(3);
        for (let k = 0; k < ref.length; k += 4) {
            const e = ref[k];
            if (typeof e === "number") {
                total += e;
            }
            else {
                // Don`t know if my fault but probably should be an Error
                throw new TypeError("Critical Error in killsoid");
            }
        }
        // NOW. If the last reply of the multi statement is 0 then this was the last session.
        // We need to remove the ZSET for this user also:
        if (response.at(-1) === 0) {
            await this.redis.zRem(`${this.redisns}${options.app}:_users`, options.id);
        }
        return { kill: total };
    }
    // Ping
    //
    // Ping the Redis server
    async ping() {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        return await this.redis.ping();
    }
    // Quit
    //
    // Quit the Redis connection
    // This is needed if Redis-Session is used with AWS Lambda.
    async quit() {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        if (this.wiperInterval !== null) {
            clearInterval(this.wiperInterval);
        }
        await this.redis.quit();
    }
    /* Set

     Set/Update/Delete custom data for a single session.
     All custom data is stored in the `d` object which is a simple hash object structure.

     `d` might contain **one or more** keys with the following types: `string`, `number`, `boolean`, `null`.
     Keys with all values except `null` will be stored. If a key containts `null` the key will be removed.

     Note: If `d` already contains keys that are not supplied in the set request then these keys will be untouched.

     **Parameters:**

     An object with the following keys:

     * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
     * `token` must be [a-zA-Z0-9] and 64 chars long
     * `d` must be an object with keys whose values only consist of strings, numbers, boolean and null.
     * `no_resave` *optional* Default: false. Boolean if true ttl will not refresh
    */
    async set(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, [
            "app",
            "token",
            "d",
            "no_resave"
        ]);
        const getOptions = {
            app: options.app,
            d: options.d,
            token: options.token,
            _noupdate: true,
            _nocache: true
        };
        // Get the session
        let resp = await this.get(getOptions);
        if (!resp) {
            return null;
        }
        // Cleanup `d`
        const nullkeys = [];
        for (const e of Object.keys(options.d)) {
            if (options.d[e] === null) {
                nullkeys.push(e);
            }
        }
        // OK ready to set some data
        if (resp.d) {
            resp.d = lodash_1.default.extend(lodash_1.default.omit(resp.d, nullkeys), lodash_1.default.omit(options.d, nullkeys));
        }
        else {
            resp.d = lodash_1.default.omit(options.d, nullkeys);
        }
        // We now have a cleaned version of resp.d ready to save back to Redis.
        // If resp.d contains no keys we want to delete the `d` key within the hash though.
        const thekey = `${this.redisns}${options.app}:${options.token}`;
        const mc = this._createMultiStatement(options.app, options.token, resp.id, resp.ttl, resp.no_resave);
        mc.hIncrBy(thekey, "w", 1);
        // Only update the `la` (last access) value if more than 1 second idle
        if (resp.idle > 1) {
            mc.hSet(thekey, "la", this._now());
        }
        if (lodash_1.default.keys(resp.d).length > 0) {
            mc.hSet(thekey, "d", JSON.stringify(resp.d));
        }
        else {
            mc.hDel(thekey, "d");
            resp = lodash_1.default.omit(resp, "d");
        }
        if (this.isCache) {
            if (!this.subscribed) {
                this.subscribed = await this.toSubscribe;
            }
            mc.publish(`${this.redisns}cache`, `${options.app}:${options.token}`);
        }
        const reply = await mc.exec();
        if (typeof reply[3] === "number") {
            resp.w = reply[3];
        }
        else {
            throw new TypeError("Critical Error Set Option");
        }
        return resp;
    }
    /* Session of App

     Returns all sessions of a single app that were active within the last *n* seconds
     Note: This might return a lot of data depending on `deltaTime`. Use with care.

     **Parameters:**

     * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
     * `deltaTime` Delta time. Amount of seconds to check (e.g. 600 for the last 10 min.)
    */
    async soapp(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "deltaTime"]);
        // https://redis.io/commands/zrevrangebyscore/
        const resp = await this.redis.zRange(`${this.redisns}${options.app}:_sessions`, "+inf", this._now() - options.deltaTime, {
            BY: "SCORE",
            REV: true
        });
        const result = [];
        for (const e of resp) {
            result.push(e.split(":")[0]);
        }
        return this._returnSessions(options, result);
    }
    /* Sessions of ID (soid)

     Returns all sessions of a single id

     **Parameters:**

     An object with the following keys:

     * `app` must be [a-zA-Z0-9_-] and 3-20 chars long
     * `id` must be [a-zA-Z0-9_-] and 1-64 chars long
    */
    async soid(options) {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        this._validate(options, ["app", "id"]);
        const resp = await this.redis.sMembers(`${this.redisns}${options.app}:us:${options.id}`);
        return await this._returnSessions(options, resp);
    }
    // Helpers
    // to handle async work of constructor
    async connect() {
        await this.redis.connect();
        return true;
    }
    _createMultiStatement = (app, token, id, ttl, no_resave) => {
        const now = this._now();
        const multi = this.redis.multi();
        multi.zAdd(`${this.redisns}${app}:_sessions`, { score: now, value: `${token}:${id}` });
        multi.zAdd(`${this.redisns}${app}:_users`, { score: now, value: id });
        multi.zAdd(`${this.redisns}SESSIONS`, { score: now + ttl, value: `${app}:${token}:${id}` });
        if (no_resave) {
            multi.hSet(`${this.redisns}${app}:${token}`, "ttl", ttl);
        }
        return multi;
    };
    _createToken = () => {
        let t = "";
        // Note we don't use Z as a valid character here
        const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYabcdefghijklmnopqrstuvwxyz0123456789";
        for (let i = 0; i < 55; i++) {
            t += possible.charAt((0, node_crypto_1.randomInt)(0, possible.length));
        }
        // add the current time in ms to the very end seperated by a Z
        return t + "Z" + Date.now().toString(36);
    };
    // returns new Error
    _handleError(err, data) {
        // try to create a error Object with humanized message
        if (lodash_1.default.isString(err)) {
            const _err = new Error(err);
            _err.name = err;
            if ("msg" in data) {
                _err.message = data.msg;
            }
            else {
                if (err === "missingParameter") {
                    _err.message = `No ${data.item} supplied`;
                }
                else {
                    _err.message = `Invalid ${data.item} format`;
                }
            }
            return _err;
        }
        return new Error(err);
    }
    // returns current timestamp in seconds
    _now() {
        return Number.parseInt("" + (Date.now() / 1000), 10);
    }
    // takes redis response and builds the corresponding session object
    _prepareSession(session, token) {
        if (session[0] === null) {
            return null;
        }
        const now = this._now();
        // Create the return object
        const o = {
            id: session[0].toString(),
            token: token,
            r: Number(session[1]),
            w: Number(session[2]),
            ttl: Number(session[3]),
            idle: now - Number(session[5]),
            ip: `${session[6]}`,
        };
        // Oh wait. If o.ttl < o.idle we need to bail out.
        if (o.ttl < o.idle) {
            // We return an empty session object
            return null;
        }
        // Support for `no_resave` #36
        if (session[7] === "1") {
            o.no_resave = true;
            o.ttl = o.ttl - o.idle;
        }
        // Parse the content of `d`
        if (session[4]) {
            o.d = JSON.parse(session[4]);
        }
        return o;
    }
    async _returnSessions(options, sessions) {
        if (sessions.length === 0) {
            return { sessions: [] };
        }
        const mc = this.redis.multi();
        for (const e of sessions) {
            mc.hmGet(`${this.redisns}${options.app}:${e}`, [
                "id",
                "r",
                "w",
                "ttl",
                "d",
                "la",
                "ip",
                "no_resave"
            ]);
        }
        const resp = await mc.exec();
        const o = [];
        for (const [i, e] of resp.entries()) {
            if (Array.isArray(e)) {
                const result = [];
                for (const reply of e) {
                    if (typeof reply === "string" || typeof reply === "number") {
                        result.push(reply.toString());
                    }
                    else if (reply === null) {
                        result.push(reply);
                    }
                    else {
                        throw new Error("Critical Error in return Session");
                    }
                }
                const session = this._prepareSession(result, sessions[i]);
                if (session) {
                    o.push(session);
                }
            }
            else {
                throw new TypeError("Critical Error in return Session2");
            }
        }
        return { sessions: o };
    }
    // handle redissub from constructor because of async
    async subscribe() {
        if (this.redissub) {
            await this.redissub.connect();
            await this.redissub.subscribe(`${this.redisns}cache`, (message, _channel) => {
                if (this.sessionCache) {
                    this.sessionCache.delete(message);
                }
                return;
            });
        }
        return true;
    }
    // Validation regex used by _validate
    VALID = {
        app: /^([\w-]){3,20}$/,
        id: /^(.*?){1,128}$/,
        ip: /^.{1,39}$/,
        token: /^([\dA-Za-z]){64}$/
    };
    // trows an Error if user input isn`t following rules
    _validate(o, items) {
        for (const item of items) {
            switch (item) {
                case "app":
                case "id":
                case "ip":
                case "token": {
                    const value = o[item];
                    if (!value) {
                        throw this._handleError("missingParameter", { item: item });
                    }
                    if (!this.VALID[item].test(value)) {
                        throw this._handleError("invalidFormat", { item: item });
                    }
                    break;
                }
                case "ttl": {
                    const ttl = Number.parseInt(o.ttl ? `${o.ttl}` : "7200", 10);
                    if (lodash_1.default.isNaN(ttl) || !lodash_1.default.isNumber(ttl) || ttl < 10) {
                        throw this._handleError("invalidValue", { msg: "ttl must be a positive integer >= 10" });
                    }
                    break;
                }
                case "no_resave": {
                    break;
                }
                case "deltaTime": {
                    const dt = Number.parseInt(`${o[item]}`, 10);
                    if (lodash_1.default.isNaN(dt) || !lodash_1.default.isNumber(dt) || dt < 10) {
                        throw this._handleError("invalidValue", { msg: "deltaTime must be a positive integer >= 10" });
                    }
                    break;
                }
                case "d": {
                    if (!o[item]) {
                        throw this._handleError("missingParameter", { item: item });
                    }
                    if (!lodash_1.default.isObject(o.d) || lodash_1.default.isArray(o.d)) {
                        throw this._handleError("invalidValue", { msg: "d must be an object" });
                    }
                    const keys = lodash_1.default.keys(o.d);
                    if (keys.length === 0) {
                        throw this._handleError("invalidValue", { msg: "d must containt at least one key." });
                    }
                    // Check if every key is either a boolean, string or a number
                    // for (const e of Object.keys(o.d)) {
                    // 	if (!_.isString(o.d[e]) && !_.isNumber(o.d[e]) && !_.isBoolean(o.d[e]) && !_.isNull(o.d[e])) {
                    // 		throw this._handleError("invalidValue", { msg: `d.${e} has a forbidden type. Only strings, numbers, boolean and null are allowed.` });
                    // 	}
                    // }
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }
    // Wipe old sessions
    //
    // Called by internal housekeeping every `options.wipe` seconds
    _wipe = async () => {
        if (!this.connected) {
            this.connected = await this.toConnect;
        }
        const resp = await this.redis.zRangeByScore(`${this.redisns}SESSIONS`, "-inf", this._now());
        if (resp.length > 0) {
            for (const element of resp) {
                const e = element.split(":");
                const options = {
                    app: e[0],
                    token: e[1],
                    id: e[2]
                };
                await this._kill(options);
            }
        }
        return;
    };
}
exports.default = RedisSessions;
