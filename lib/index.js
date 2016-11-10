'use strict';

/**
 * Dependencies
 */
const Promise = require('bluebird');
const debug = require('debug')('node-coal');
const redis = require('redis');
const parse = require('parse-redis-url');
const error = require('./errors');

/**
 * Reusable functions
 */
const parser = parse(redis);

/**
 * Defaults
 */
const defaultTimeout = 100;
const defaultTTL = 30000;
const defaultLockTTL = 1000;

/**
 * Coal constructor
 * 
 * @param {String|Object} options
 */
function Coal(options) {
	options = options ? options : {};

	if (typeof options === 'string') {
		options = parse(options);
	}

	const port = options.port;
	const host = options.host;
	const client = options.client;
	const password = options.password;
	const database = options.database;
	const prefix = options.prefix;

	if (client) {
		this.client = client;
	} else if (!port && !host) {
		this.client = new redis.createClient();
	} else {
		options.prefix = null;
		this.client = new redis.createClient(port, host, options);
	}

	if (password) {
		this.client.auth(password, err => {
			if (err) throw err;
		});
	}

	if (database) {
		this.client.select(database, err => {
			if (err) throw err;
		});
	}

	this.prefix = prefix || 'coal:';
}

Coal.prototype.get = function(key, update, ttl, timeout) {
	if (typeof key !== 'string') {
		throw new TypeError('"key" should be a string');
	}

	if (typeof update !== 'function' && ttl !== undefined) {
		throw new TypeError('"update" should be a function');
	}

	if (typeof update !== 'function' && ttl === undefined) {
		ttl = update;
		update = null;
	}

	ttl = ttl * 1000 || defaultTTL;
	timeout = timeout || ((ttl > 0) ? ttl / 10 : defaultTimeout);

	const k = `${this.prefix}${key}`;

	return new Promise((resolve, reject) => {
		this.client.get(k, (err, value) => {
			if (err) {
				return reject(new error.RedisError(err));
			}

			if (value === null && update !== null) {
				
				update((err, result) => {
					if (err) {
						return reject(err);
					}

					debug(`[CACHE:UPDATE] Key: "${key}" update data`);
					this.set(key, result, ttl, timeout).then(() => {
						return resolve(result);
					});
				});
			} else {
				return resolve(value);
			}
		});
	}).then(result => {
		if (result !== null && typeof result !== 'object') {
			try {
				result = JSON.parse(result);
			} catch (e) {
				// Just supress wrong parse. It possible not a JSON at all
			}
		}

		debug(`[CACHE:GET] Key: "${key}" Data: "${JSON.stringify(result)}"`);
		return result;
	});
}

Coal.prototype.set = function(key, value, ttl, timeout) {
	if (typeof key !== 'string') {
		throw new TypeError('"key" should be a string');
	}

	if (typeof value === 'undefined') {
		throw new TypeError('"value" should be defined');
	}

	if (typeof value === 'object') {
		try {
			value = JSON.stringify(value);
		} catch (e) {
			throw new Error(e);
		}
	} else {
		value = value.toString();
	}

	ttl = ttl * 1000 || defaultTTL;
	timeout = timeout || (( ttl > 0 ) ? ttl / 10 : defaultTimeout);
	
	const retryDelay = Math.floor(Math.random() * (30 - 10 + 1)) + 10;
	const k = `${this.prefix}${key}`;
	const lock = `${this.prefix}lock:${key}`;

	return new Promise((resolve, reject) => {
		var loop = null;
		var attempts = 0;
		const start = process.hrtime();

		const acquireLock = () => {
			attempts++;
			const diff = process.hrtime(start);
			const elapsed = parseFloat((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(4);

			if (elapsed > timeout) {
				debug(`[LOCK:ERROR] Couldn't obtain lock in ${timeout} ms`);

				if (loop !== null) {
					clearInterval(loop);
				}
				
				return reject(new error.AcquireError('Error while obtaining lock to cache'));
			}

			this.client.set(lock, 1, 'NX', 'PX', timeout + 500, (err, result) => {
				debug(`[LOCK:SET] Key: "${lock}", Attempt: ${attempts}, Elapsed: ${elapsed}ms.`);

				if (err) {
					if (loop !== null) {
						clearInterval(loop);
					}
					
					return reject(new error.RedisError(err));
				}

				if (result !== null) {
					debug(`[LOCK:OBTAINED] Key: "${lock}", Attempts: ${attempts}, Elapsed: ${elapsed}ms.`);

					if (loop !== null) {
						clearInterval(loop);
					}
					
					return resolve({
						elapsed: elapsed,
						attempts: attempts
					});
				}
			});
		}
		
		// Launching spinlock
		acquireLock();
		loop = setInterval(acquireLock, retryDelay);
	}).then(lockStats => {		
		return new Promise((resolve, reject) => {
			const cb = (err, result) => {
				if (err) {
					return reject(new error.RedisError(err));
				}

				debug(`[CACHE:SET] Key: "${k}" set "${value}"`);

				// Removing lock
				this.client.del(lock, (err, data) => {
					if (err) {
						return reject(new error.RedisError(err));
					}
					
					debug(`[LOCK:DEL] Key: "${lock}" removed lock `);

					return resolve(lockStats);
				});
			};

			if (ttl > 0) {
				this.client.set(k, value, 'PX', ttl, cb);
			} else {
				this.client.set(k, value, cb);
			}
		});
	});
}

Coal.prototype.del = function(key) {
	const k = `${this.prefix}${key}`;

	return new Promise((resolve, reject) => {
		this.client.del(k, (err, data) => {
			if (err) {
				return reject(err);
			}

			return resolve(data);
		});
	});
}

module.exports = Coal;