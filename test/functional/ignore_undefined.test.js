'use strict';
var test = require('./shared').assert;
const { expect } = require('chai');
var setupDatabase = require('./shared').setupDatabase;
const { ObjectId } = require('../../src');

describe('Ignore Undefined', function () {
  before(function () {
    return setupDatabase(this.configuration);
  });

  it('Should correctly insert document ignoring undefined field', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1,
        ignoreUndefined: true
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db);
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue');

        // Ignore the undefined field
        collection.insert({ a: 1, b: undefined }, configuration.writeConcernMax(), function (err) {
          expect(err).to.not.exist;

          // Locate the doument
          collection.findOne(function (err, item) {
            test.equal(1, item.a);
            test.ok(item.b === undefined);
            client.close(done);
          });
        });
      });
    }
  });

  it(
    'Should correctly connect using MongoClient and perform insert document ignoring undefined field',
    {
      metadata: { requires: { topology: ['single'] } },

      test: function (done) {
        var configuration = this.configuration;
        const client = configuration.newClient(
          {},
          {
            bufferMaxEntries: 0,
            ignoreUndefined: true,
            sslValidate: false
          }
        );

        client.connect(function (err, client) {
          var db = client.db(configuration.db);
          var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue1');
          collection.insert({ a: 1, b: undefined }, function (err) {
            expect(err).to.not.exist;

            collection.findOne(function (err, item) {
              test.equal(1, item.a);
              test.ok(item.b === undefined);

              collection.insertOne({ a: 2, b: undefined }, function (err) {
                expect(err).to.not.exist;

                collection.findOne({ a: 2 }, function (err, item) {
                  test.equal(2, item.a);
                  test.ok(item.b === undefined);

                  collection.insertMany([{ a: 3, b: undefined }], function (err) {
                    expect(err).to.not.exist;

                    collection.findOne({ a: 3 }, function (err, item) {
                      test.equal(3, item.a);
                      test.ok(item.b === undefined);
                      client.close(done);
                    });
                  });
                });
              });
            });
          });
        });
      }
    }
  );

  it('Should correctly update document ignoring undefined field', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1,
        ignoreUndefined: true
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db);
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue2');
        var id = new ObjectId();

        collection.updateOne(
          { _id: id, a: 1, b: undefined },
          { $set: { a: 1, b: undefined } },
          { upsert: true },
          function (err) {
            expect(err).to.not.exist;
            collection.findOne({ _id: id }, function (err, item) {
              test.equal(1, item.a);
              test.ok(item.b === undefined);
              var id = new ObjectId();

              collection.updateMany(
                { _id: id, a: 1, b: undefined },
                { $set: { a: 1, b: undefined } },
                { upsert: true },
                function (err) {
                  expect(err).to.not.exist;
                  collection.findOne({ _id: id }, function (err, item) {
                    test.equal(1, item.a);
                    test.ok(item.b === undefined);
                    var id = new ObjectId();

                    collection.update(
                      { _id: id, a: 1, b: undefined },
                      { $set: { a: 1, b: undefined } },
                      { upsert: true },
                      function (err) {
                        expect(err).to.not.exist;
                        collection.findOne({ _id: id }, function (err, item) {
                          test.equal(1, item.a);
                          test.ok(item.b === undefined);
                          client.close(done);
                        });
                      }
                    );
                  });
                }
              );
            });
          }
        );
      });
    }
  });

  it('Should correctly inherit ignore undefined field from db during insert', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1,
        ignoreUndefined: false
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db, { ignoreUndefined: true });
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue3');

        // Ignore the undefined field
        collection.insert({ a: 1, b: undefined }, configuration.writeConcernMax(), function (err) {
          expect(err).to.not.exist;

          // Locate the doument
          collection.findOne(function (err, item) {
            test.equal(1, item.a);
            test.ok(item.b === undefined);
            client.close(done);
          });
        });
      });
    }
  });

  it('Should correctly inherit ignore undefined field from collection during insert', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db, { ignoreUndefined: false });
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue4', {
          ignoreUndefined: true
        });

        // Ignore the undefined field
        collection.insert({ a: 1, b: undefined }, configuration.writeConcernMax(), function (err) {
          expect(err).to.not.exist;

          // Locate the doument
          collection.findOne(function (err, item) {
            test.equal(1, item.a);
            test.ok(item.b === undefined);
            client.close(done);
          });
        });
      });
    }
  });

  it('Should correctly inherit ignore undefined field from operation during insert', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db);
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue5', {
          ignoreUndefined: false
        });

        // Ignore the undefined field
        collection.insert({ a: 1, b: undefined }, { ignoreUndefined: true }, function (err) {
          expect(err).to.not.exist;

          // Locate the doument
          collection.findOne({}, function (err, item) {
            expect(err).to.not.exist;
            test.equal(1, item.a);
            test.ok(item.b === undefined);
            client.close(done);
          });
        });
      });
    }
  });

  it('Should correctly inherit ignore undefined field from operation during findOneAndReplace', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1,
        ignoreUndefined: false
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db);
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue6');

        collection.insert({ a: 1, b: 2 }, configuration.writeConcernMax(), function (err) {
          expect(err).to.not.exist;

          // Replace the doument, ignoring undefined fields
          collection.findOneAndReplace(
            {},
            { a: 1, b: undefined },
            { ignoreUndefined: true },
            function (err) {
              expect(err).to.not.exist;

              // Locate the doument
              collection.findOne(function (err, item) {
                test.equal(1, item.a);
                test.ok(item.b === undefined);
                client.close(done);
              });
            }
          );
        });
      });
    }
  });

  it('Should correctly ignore undefined field during bulk write', {
    metadata: { requires: { topology: ['single'] } },

    test: function (done) {
      var configuration = this.configuration;
      var client = configuration.newClient(configuration.writeConcernMax(), {
        poolSize: 1
      });

      client.connect(function (err, client) {
        var db = client.db(configuration.db);
        var collection = db.collection('shouldCorrectlyIgnoreUndefinedValue7');

        // Ignore the undefined field
        collection.bulkWrite(
          [{ insertOne: { a: 0, b: undefined } }],
          { ignoreUndefined: true },
          function (err) {
            expect(err).to.not.exist;

            // Locate the doument
            collection.findOne(function (err, item) {
              test.equal(0, item.a);
              test.ok(item.b === undefined);
              client.close(done);
            });
          }
        );
      });
    }
  });
});
