'use strict';

const util = require('util');

function RedisError(message) {
	Error.captureStackTrace(this, RedisError);
	this.name = 'RedisError';
	this.message = message;
}

function AcquireError(message) {
	Error.captureStackTrace(this, AcquireError);
	this.name = 'AcquireError';
	this.message = message;
}

util.inherits(AcquireError, Error);
util.inherits(RedisError, Error);

exports.AcquireError = AcquireError;
exports.RedisError = RedisError;

