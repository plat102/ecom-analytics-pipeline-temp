// ============================================================
// List all fields across all event types
// Strategy: 10 docs per event type, all 27 event types
// ============================================================

// use countly;

// --- Sampling by event type ---
var fields = {};
db.summary.distinct("collection").forEach(function(eventType) {
    db.summary.find({collection: eventType}).limit(10).forEach(function(doc) {
        Object.keys(doc).forEach(function(key) {
            if (!fields[key]) fields[key] = typeof doc[key];
        });
    });
});
printjson(fields);

// --- ALTERNATIVE: Get fields + all possible types (more detail) ---
// Useful when a field can be multiple types (e.g. string | null | array)
//
// db.summary.aggregate([
//   { $sample: { size: 1000 } },
//   { $project: { fields: { $objectToArray: "$$ROOT" } } },
//   { $unwind: "$fields" },
//   { $group: {
//       _id: "$fields.k",
//       types: { $addToSet: { $type: "$fields.v" } }
//   }},
//   { $sort: { _id: 1 } }
// ]);
// NOTE: Use $sample size 100k to catch rare event fields
