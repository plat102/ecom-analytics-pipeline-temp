// ============================================================
// Calculate null/missing rates for key fields
// Sample: 50k docs (0.12% of 41M)
// ============================================================
// use countly;

// Example: Check null rate for 2 fields (ip, product_id)
// For full analysis, use Python script: 05_null_rates.py
db.summary.aggregate([
  { $sample: { size: 50000 } },
  {
    $group: {
      _id: null,
      total: { $sum: 1 },
      ip_null: {
        $sum: {
          $cond: [
            { $or: [
              { $eq: ["$ip", null] },
              { $eq: ["$ip", ""] },
              { $not: ["$ip"] }
            ]},
            1,
            0
          ]
        }
      },
      product_id_null: {
        $sum: {
          $cond: [
            { $or: [
              { $eq: ["$product_id", null] },
              { $eq: ["$product_id", ""] },
              { $not: ["$product_id"] }
            ]},
            1,
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      total: 1,
      ip_null_rate: { $multiply: [{ $divide: ["$ip_null", "$total"] }, 100] },
      product_id_null_rate: { $multiply: [{ $divide: ["$product_id_null", "$total"] }, 100] }
    }
  }
]);

// --- To add more fields, copy this pattern: ---
// field_name_null: {
//   $sum: {
//     $cond: [
//       { $or: [
//         { $eq: ["$field_name", null] },
//         { $eq: ["$field_name", ""] },
//         { $not: ["$field_name"] }
//       ]},
//       1,
//       0
//     ]
//   }
// }
