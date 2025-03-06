db = db.getSiblingDB("admin");
db.createUser({
  user: "admin",
  pwd: "admin",
  roles: [{ role: "root", db: "admin" }]
});

db = db.getSiblingDB("used_cars");
db.createUser({
  user: "metabase",
  pwd: "metabase",
  roles: [{ role: "readWrite", db: "used_cars" }]
});
