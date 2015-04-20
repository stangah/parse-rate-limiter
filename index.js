//  Parse rate limiter v0.0.1

//  Parse rate limiter may be freely distributed under the MIT license


/**
 * This function was written as a way to throttle requests to the Parse API
 * without completely serializing it. It will control the amount of objects
 * to be saved per second according to the input rate
 *
 * Example:
 *
 *   var object1 = new Parse.Object('SomeClass');
 *   var object2 = new Parse.Object('SomeClass');
 *   var object3 = new Parse.Object('SomeClass');
 *   var objects = [object2, object3];
 *
 *   var rateLimiter = new ParseRateLimiter(30) // <-- objects to be saved per second
 *   rateLimiter.save(object);
 *   rateLimiter.saveAll(objects);
 *
 *   return rateLimiter.finalize();
 *
 */

(function(root, factory) {
    'use strict';

    // Export for node or put in window for browser
    if (typeof define === 'function' && define.amd) {
        define(function(require) {
            var Parse = require('parse');
            var _ = require('underscore');
            root.ParseRateLimiter = factory(Parse, _);
        });
    } else if (typeof exports !== 'undefined') {
        var Parse = require('parse').Parse;
        var _ = require('underscore');
        module.exports = factory(Parse, _);
    } else {
        root.ParseRateLimiter = factory(root.Parse, root._);
    }

}.call(this, this, function(Parse, _) {

    var ParseRateLimiter = function(maxRate) {

        var self = this;
        var finalizedPromise;
        var toSave;
        var timer;
        var savePromise;
        self._maxRate = maxRate;
        self._killSwitch = false;
        self._saveQueue = [];

        var errHandler = function(err) {
            if(self._killSwitch) { return; }

            console.log('Save error in ParseRateLimiter!!!  D:');
            console.log(err);

            // Clear the queue and reject the returned promise
            clearTimeout(timer);
            self._killSwitch = true;
            self._saveQueue = [];
            if(finalizedPromise) {
                finalizedPromise.reject(err);
            } else {
                self._error = err;
            }
        };

        // Saving using default batch size (20 using Parse's _deepSaveAsync)
        // can create requests too large for Parse to handle
        var saveInBatches = function(objects) {
            var toSave;
            var batchSize = 10;
            var promises = [];

            while(objects.length) {
                toSave = _.first(objects, batchSize);
                objects = _.rest(objects, batchSize);
                promises.push(Parse.Object.saveAll(toSave));
            }

            return Parse.Promise.when(promises);
        };

        var rateLimiter = function() {
            if(self.killswitch) { return; }

            if(self._saveQueue.length) {

                // Lops off a batch of size determined by desired request rate
                toSave = _.first(self._saveQueue, self._maxRate);
                self._saveQueue = _.rest(self._saveQueue, self._maxRate);
                savePromise = saveInBatches(toSave).fail(errHandler);

                console.log(self._saveQueue.length + ' items left in the queue.');

                if(!self._saveQueue.length) {
                    timer = null;

                    if(finalizedPromise) {
                        // Tack on a finalized promise resolution to only the last batch save
                        savePromise.then(function() {
                            finalizedPromise.resolve();
                        });
                    }
                } else {
                    // Save another batch after waiting
                    timer = setTimeout(_.bind(rateLimiter, this), 1000);
                }
            } else if(timer) {
                // If the queue is empty and we're almost done, clear the timer
                timer = null;
            }
        };

        self._startTimer = function() {
            timer = timer || setTimeout(_.bind(rateLimiter, this), 0);
        };

        self.save = function(object) {
            if(!(object instanceof Parse.Object)) {
                throw new Error('save must accept a Parse Object');
            }

            self._saveQueue.push(object);
        };

        self.saveAll = function(objects) {
            var inputError;

            // Make sure it's an array
            if(!Array.isArray(objects)) {
                inputError = 'saveAll must accept an array of objects';
            }

            // Make sure the array contains only parse objects
            var notParseObject = _.find(objects, function(object) {
                return !(object instanceof Parse.Object);
            });
            if(notParseObject) {
                inputError = 'Cannot process item that is not a Parse object';
                console.log('The following item failed input validation:');
                console.log(notParseObject);
            }

            if(inputError) { throw new Error(inputError); }

            // Add the items to the queue
            self._saveQueue = self._saveQueue.concat(objects);
        };

        self.finalize = function() {
            console.log('Finalizing, kicking off rate limited saves');

            if(self._error) {
                // If we've already received an error, the save process has been
                // killed and we should immediately return the error
                return Parse.Promise.error(self._error);
            } else if(self._saveQueue.length) {
                // There are things left to be saved, so start saving and return
                // a deferred promise
                self._startTimer();

                finalizedPromise = new Parse.Promise();
                return finalizedPromise;
            } else {
                // If the queue is empty, we must have called finalize after
                // the queue was cleared, but a save might still be occurring
                return savePromise.then(
                    function() {
                        return Parse.Promise.as();
                    }
                );
            }
        };
    };

    return ParseRateLimiter;

}));
