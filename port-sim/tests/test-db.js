const {
  splitEdgeDocument,
  mergeEdgeSnapshot,
  RUNTIME_COLLECTION,
  ASSIGNMENT_COLLECTION,
  LEGACY_COLLECTION,
} = require('../lib/edge-collections');

function createSplitEdgeDb(initial = {}) {
  const runtimeStore = new Map();
  const assignmentStore = new Map();

  for (const [id, doc] of Object.entries(initial)) {
    const { runtime, assignment } = splitEdgeDocument({ id, ...doc });
    runtimeStore.set(id, { ...runtime });
    assignmentStore.set(id, { ...assignment });
  }

  function collectionApi(store, allowUpsert = false) {
    return {
      async findOne(query) {
        const doc = store.get(query.id);
        return doc ? { ...doc } : null;
      },
      async updateOne(query, update, options = {}) {
        const id = query.id;
        let current = store.get(id);
        if (!current && options.upsert) {
          current = { id };
        }
        if (!current) {
          return { modifiedCount: 0 };
        }
        if (query['activeHop.startedAt'] && current.activeHop?.startedAt !== query['activeHop.startedAt']) {
          return { modifiedCount: 0 };
        }
        if (update.$set) Object.assign(current, update.$set);
        if (update.$inc?.stateRevision) {
          current.stateRevision = (current.stateRevision || 0) + update.$inc.stateRevision;
        }
        store.set(id, current);
        return { modifiedCount: 1 };
      },
      async countDocuments() {
        return store.size;
      },
      find() {
        const docs = [...store.values()].map((doc) => ({ ...doc }));
        return {
          toArray: async () => docs,
        };
      },
    };
  }

  return {
    collection(name) {
      if (name === 'edgeHistory') {
        return {
          async insertOne() {},
          async findOne() { return null; },
          async updateOne() {},
          find() { return { toArray: async () => [] }; },
        };
      }
      if (name === RUNTIME_COLLECTION || name === LEGACY_COLLECTION) {
        return collectionApi(runtimeStore, name === RUNTIME_COLLECTION);
      }
      if (name === ASSIGNMENT_COLLECTION) {
        return collectionApi(assignmentStore, true);
      }
      throw new Error(`Unexpected collection ${name}`);
    },
    get(id) {
      return mergeEdgeSnapshot(assignmentStore.get(id), runtimeStore.get(id));
    },
    getRuntime(id) {
      return runtimeStore.get(id);
    },
    getAssignment(id) {
      return assignmentStore.get(id);
    },
  };
}

module.exports = {
  createSplitEdgeDb,
};
