
// Count unique IPs using aggregation
// db.summary.aggregate([
// { $group: { _id: "$ip" } },
// { $count: "unique_ips" }
// ])



// Create index on ip field (ascending)
db.summary.createIndex({ "ip": 1 });

db.summary.getIndexes();
