'use strict';
const { setupDatabase, withClient } = require('./shared');
const chai = require('chai');
const expect = chai.expect;

describe('Explain', function () {
  before(function () {
    return setupDatabase(this.configuration);
  });

  it('should honor boolean explain with delete one', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithDeleteOne');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.deleteOne({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with delete many', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithDeleteMany');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.deleteMany({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with update one', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithUpdateOne');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.updateOne(
          { a: 1 },
          { $inc: { a: 2 } },
          { explain: true },
          (err, explanation) => {
            expect(err).to.not.exist;
            expect(explanation).to.exist;
            expect(explanation).property('queryPlanner').to.exist;
            done();
          }
        );
      });
    })
  });

  it('should honor boolean explain with update many', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithUpdateMany');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.updateMany(
          { a: 1 },
          { $inc: { a: 2 } },
          { explain: true },
          (err, explanation) => {
            expect(err).to.not.exist;
            expect(explanation).to.exist;
            expect(explanation).nested.property('queryPlanner').to.exist;
            done();
          }
        );
      });
    })
  });

  it('should honor boolean explain with remove one', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithRemoveOne');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.removeOne({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with remove many', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithRemoveMany');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.removeMany({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with distinct', {
    metadata: {
      requires: {
        mongodb: '>=3.2'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithDistinct');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.distinct('a', {}, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with findOneAndModify', {
    metadata: {
      requires: {
        mongodb: '>=3.2'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithFindOneAndModify');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.findOneAndDelete({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('should honor boolean explain with mapReduce', {
    metadata: {
      requires: {
        mongodb: '>=4.4'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorBooleanExplainWithMapReduce');
      var collection = db.collection('test');

      collection.insertMany([{ user_id: 1 }, { user_id: 2 }], (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        var map = 'function() { emit(this.user_id, 1); }';
        var reduce = 'function(k,vals) { return 1; }';

        collection.mapReduce(
          map,
          reduce,
          { out: { replace: 'tempCollection' }, explain: true },
          (err, explanation) => {
            expect(err).to.not.exist;
            expect(explanation).to.exist;
            expect(explanation).property('stages').to.exist;
            done();
          }
        );
      });
    })
  });

  it('should use allPlansExecution as true explain verbosity', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldUseAllPlansExecutionAsTrueExplainVerbosity');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        // Verify explanation result contains properties of allPlansExecution output
        collection.deleteOne({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).nested.property('executionStats.allPlansExecution').to.exist;
          done();
        });
      });
    })
  });

  it('should use queryPlanner as false explain verbosity', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldUseQueryPlannerAsFalseExplainVerbosity');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        // Verify explanation result contains properties of queryPlanner output
        collection.deleteOne({ a: 1 }, { explain: false }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).to.not.have.property('executionStats');
          done();
        });
      });
    })
  });

  it('should honor queryPlanner string explain', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorQueryPlannerStringExplain');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        // Verify explanation result contains properties of queryPlanner output
        collection.deleteOne({ a: 1 }, { explain: 'queryPlanner' }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).to.not.have.property('executionStats');
          done();
        });
      });
    })
  });

  it('should honor executionStats string explain', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorExecutionStatsStringExplain');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        // Verify explanation result contains properties of executionStats output
        collection.deleteMany({ a: 1 }, { explain: 'executionStats' }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).property('executionStats').to.exist;
          expect(explanation.executionStats).to.not.have.property('allPlansExecution');
          done();
        });
      });
    })
  });

  it('should honor allPlansExecution string explain', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorAllPlansStringExplain');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        // Verify explanation result contains properties of allPlansExecution output
        collection.removeOne({ a: 1 }, { explain: 'allPlansExecution' }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).nested.property('executionStats.allPlansExecution').to.exist;
          done();
        });
      });
    })
  });

  it('should honor string explain with distinct', {
    metadata: {
      requires: {
        mongodb: '>=3.2'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorStringExplainWithDistinct');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.distinct('a', {}, { explain: 'executionStats' }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).property('executionStats').to.exist;
          done();
        });
      });
    })
  });

  it('should honor string explain with findOneAndModify', {
    metadata: {
      requires: {
        mongodb: '>=3.2'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorStringExplainWithFindOneAndModify');
      var collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.findOneAndReplace(
          { a: 1 },
          { a: 2 },
          { explain: 'queryPlanner' },
          (err, explanation) => {
            expect(err).to.not.exist;
            expect(explanation).to.exist;
            expect(explanation).property('queryPlanner').to.exist;
            done();
          }
        );
      });
    })
  });

  it('should honor string explain with mapReduce', {
    metadata: {
      requires: {
        mongodb: '>=4.4'
      }
    },
    test: withClient(function (client, done) {
      var db = client.db('shouldHonorStringExplainWithMapReduce');
      var collection = db.collection('test');

      collection.insertMany([{ user_id: 1 }, { user_id: 2 }], (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        var map = 'function() { emit(this.user_id, 1); }';
        var reduce = 'function(k,vals) { return 1; }';

        collection.mapReduce(
          map,
          reduce,
          { out: { replace: 'tempCollection' }, explain: 'executionStats' },
          (err, explanation) => {
            expect(err).to.not.exist;
            expect(explanation).to.exist;
            expect(explanation).property('stages').to.exist;
            done();
          }
        );
      });
    })
  });

  it('shouldHonorBooleanExplainWithFind', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainWithFind');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.find({ a: 1 }, { explain: true }).toArray((err, docs) => {
          expect(err).to.not.exist;
          const explanation = docs[0];
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorStringExplainWithFind', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorStringExplainWithFind');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.find({ a: 1 }, { explain: 'executionStats' }).toArray((err, docs) => {
          expect(err).to.not.exist;
          const explanation = docs[0];
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).property('executionStats').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorBooleanExplainWithFindOne', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainWithFindOne');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.findOne({ a: 1 }, { explain: true }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorStringExplainWithFindOne', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorStringExplainWithFindOne');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection.findOne({ a: 1 }, { explain: 'executionStats' }, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).property('executionStats').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorBooleanExplainSpecifiedOnCursorWithFind', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainSpecifiedOnCursor');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        const cursor = collection.find({ a: 1 });
        cursor.explain(false, (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorStringExplainSpecifiedOnCursorWithFind', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainSpecifiedOnCursor');
      const collection = db.collection('test');
      const cursor = collection.find({ a: 1 });

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        cursor.explain('allPlansExecution', (err, explanation) => {
          expect(err).to.not.exist;
          expect(explanation).to.exist;
          expect(explanation).property('queryPlanner').to.exist;
          expect(explanation).property('executionStats').to.exist;
          done();
        });
      });
    })
  });

  it('shouldHonorLegacyExplainWithFind', {
    metadata: {
      requires: {
        mongodb: '<3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorLegacyExplainWithFind');
      const collection = db.collection('test');
      const cursor = collection.find({ a: 1 });

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        cursor.explain((err, result) => {
          expect(err).to.not.exist;
          expect(result).to.have.property('allPlans');
          done();
        });
      });
    })
  });

  it('shouldHonorBooleanExplainWithAggregate', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainWithAggregate');
      const collection = db.collection('test');
      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection
          .aggregate([{ $project: { a: 1 } }, { $group: { _id: '$a' } }], { explain: true })
          .toArray((err, docs) => {
            expect(err).to.not.exist;
            const result = docs[0];
            expect(result).to.have.property('stages');
            expect(result.stages).to.have.lengthOf.at.least(1);
            expect(result.stages[0]).to.have.property('$cursor');
            done();
          });
      });
    })
  });

  it('shouldHonorStringExplainWithAggregate', {
    metadata: {
      requires: {
        mongodb: '>=3.6.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorStringExplainWithAggregate');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        collection
          .aggregate([{ $project: { a: 1 } }, { $group: { _id: '$a' } }], {
            explain: 'executionStats'
          })
          .toArray((err, docs) => {
            expect(err).to.not.exist;
            const result = docs[0];
            expect(result).to.have.property('stages');
            expect(result.stages).to.have.lengthOf.at.least(1);
            expect(result.stages[0]).to.have.property('$cursor');
            expect(result.stages[0].$cursor).to.have.property('queryPlanner');
            expect(result.stages[0].$cursor).to.have.property('executionStats');
            done();
          });
      });
    })
  });

  it('shouldHonorBooleanExplainSpecifiedOnCursorWithAggregate', {
    metadata: {
      requires: {
        mongodb: '>=3.0'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainSpecifiedOnCursor');
      const collection = db.collection('test');

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        const cursor = collection.aggregate([{ $project: { a: 1 } }, { $group: { _id: '$a' } }]);
        cursor.explain(false, (err, result) => {
          expect(err).to.not.exist;
          expect(result).to.have.property('stages');
          expect(result.stages).to.have.lengthOf.at.least(1);
          expect(result.stages[0]).to.have.property('$cursor');
          expect(result.stages[0].$cursor).to.have.property('queryPlanner');
          done();
        });
      });
    })
  });

  it('shouldHonorStringExplainSpecifiedOnCursorWithAggregate', {
    metadata: {
      requires: {
        mongodb: '>=3.6'
      }
    },
    test: withClient(function (client, done) {
      const db = client.db('shouldHonorBooleanExplainSpecifiedOnCursor');
      const collection = db.collection('test');
      const cursor = collection.aggregate([{ $project: { a: 1 } }, { $group: { _id: '$a' } }]);

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        cursor.explain('allPlansExecution', (err, result) => {
          expect(err).to.not.exist;
          expect(result).to.exist;
          expect(result).to.have.property('stages');
          expect(result.stages).to.have.lengthOf.at.least(1);
          expect(result.stages[0]).to.have.property('$cursor');
          expect(result.stages[0].$cursor).to.have.property('queryPlanner');
          expect(result.stages[0].$cursor).to.have.property('executionStats');
          done();
        });
      });
    })
  });

  it(
    'shouldHonorLegacyExplainWithAggregate',
    withClient(function (client, done) {
      const db = client.db('shouldHonorLegacyExplainWithAggregate');
      const collection = db.collection('test');
      const cursor = collection.aggregate([{ $project: { a: 1 } }, { $group: { _id: '$a' } }]);

      collection.insertOne({ a: 1 }, (err, res) => {
        expect(err).to.not.exist;
        expect(res).to.exist;

        cursor.explain((err, result) => {
          expect(err).to.not.exist;
          expect(result).to.have.property('stages');
          expect(result.stages).to.have.lengthOf.at.least(1);
          expect(result.stages[0]).to.have.property('$cursor');
          done();
        });
      });
    })
  );
});
