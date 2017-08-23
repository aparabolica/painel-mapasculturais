const replay = require("replay");
const _ = require("lodash");
const moment = require("moment");
var request = require("request").defaults({ jar: true });
const async = require("async");
const elementTypes = ["agent", "space", "project", "event"];

const installs = require("./config.json").installs;

const mongodbConnectionString = "mongodb://localhost:27018/painelmc";
const limitPerPage = 300;

/*
 * Init database
 */

var mongo = require("mongodb").MongoClient;
var dbConnection;
mongo.connect(mongodbConnectionString, function(err, db) {
  if (err) {
    console.log(err.message);
    return;
  }

  console.log("Connected to MongoDB.");

  dbConnection = db;
  runImport(function(err) {
    if (err) console.log(err.message);
    db.close();
  });
});

/*
 * Init import
 */
function runImport(doneRunImport) {
  async.eachSeries(
    installs,
    function(install, doneInstall) {
      // get install credentials
      const admin = install.admin;

      // set api url
      install.apiUrl = install.url + "api/";

      // authenticate if admin credentials are provided
      if (admin && admin.email != "" && admin.password != "") {
        request.post(
          install.url + "autenticacao/login",
          { form: admin },
          function(err, res) {
            if (err) return doneRunImport(err);
            else fetchInstallData(install, doneInstall);
          }
        );
      } else {
        fetchInstallData(install, doneInstall);
      }
    },
    doneRunImport
  );
}

/*
 * Fetch data for an install
 */

function fetchInstallData(install, doneFetchInstallData) {
  console.log(`Fetching data from ${install.url}`);
  async.eachSeries(
    elementTypes,
    function(type, doneType) {
      fetchTypeData(install, type, doneType);
    },
    doneFetchInstallData
  );
}

/*
 * Fetch data for a specific type
 */

function fetchTypeData(install, type, doneFetchElementType) {
  var page = 0;
  var data = [];

  var selectFields = {
    agent: "*",
    event:
      "id,createTimestamp,owner,project,classificaoEtaria,occurrences.{id,space.{*},rule},terms,type,registrationInfo,preco,traducaoLibras,descricaoSonora,site,facebook,twitter,googleplus,instagram",
    space: "*",
    project: "*"
  };

  async.doUntil(
    function(next) {
      page++;
      request(
        {
          uri: install.apiUrl + type + "/find",
          qs: {
            "@SELECT": selectFields[type],
            "@LIMIT": limitPerPage,
            "@PAGE": page
          }
        },
        function(err, res) {
          if (err) return next(err);
          else {
            // parse result
            try {
              var result = JSON.parse(res.body);
            } catch (e) {
              console.log("Error parsing response.");
              return next(e);
            }

            // append to data
            data = data.concat(result);

            // log
            if (result.length > 0)
              console.log(`${data.length} ${type}s fetched...`);

            next(null, result);
          }
        }
      );
    },
    function(result) {
      return result.length == 0;
    },
    function(err) {
      if (err) return doneFetchElementType(err);
      else importData(install, type, data, doneFetchElementType);
    }
  );
}

/*
 * Normalize city names
 */
const altName = require("./altnames.json");
function normalizeName(type, input) {
  const names = altName[type];

  if (input) {
    // Remove leading/trailing whitespaces
    input = input.trim();

    // Replaces multiple spaces with one space
    input = input.replace(/  +/g, " ");
  }

  // Do not process null or empty values
  if (!input || input == "") return null;

  // Name is correct, return unchanged
  if (names[input]) return input;

  // Try to find an alternative name
  const loweredInput = input.toLowerCase();
  for (name of Object.keys(names)) {
    if (
      name.toLowerCase() == loweredInput ||
      _.includes(names[name], loweredInput)
    )
      return name;
  }

  // Name don't need to be changed
  return input;
}

/*
 * Prepare data
 */
function preprocessData(install, type, data) {
  console.log("Parsing...");
  return _.map(data, function(d) {
    d._id = d.id;
    d.createdAt = new Date(d.createTimestamp.date);
    d.updatedAt = d.updateTimestamp ? new Date(d.updateTimestamp.date) : null;
    d.type = d.type && d.type.name;

    if (_.includes(["agent", "space"], type)) {
      d.city = normalizeName("city", d["En_Municipio"] || d["geoMunicipio"]);
      d.district = normalizeName(`district_${install.name}`, d["geoDistrito"]);
    }

    if (type == "agent") {
      d.race = d.raca != null || d.raca != "" ? d.raca : null;
      d.gender = d.genero != null || d.genero != "" ? d.genero : null;
      d.birthday = d.dataDeNascimento ? new Date(d.dataDeNascimento) : null;
      d.age = d.birthday
        ? moment().diff(moment(d.dataDeNascimento), "years")
        : null;
    }

    if (type == "space") {
      d.subspacesCount = d.children ? d.children.length : 0;
      d.acessibilidade = d.acessibilidade == "Sim" || d.acessibilidade == "Não"
        ? d.acessibilidade
        : "Não declarada";
      d.acessibilidade_fisica = d.acessibilidade_fisica
        ? d.acessibilidade_fisica.split(";")
        : null;
    }

    if (type == "event") {
      d.traducaoLibras = d.traducaoLibras == "Sim" || d.traducaoLibras == "Não"
        ? d.traducaoLibras
        : "Não declarada";
      d.descricaoSonora = d.descricaoSonora == "Sim" ||
        d.descricaoSonora == "Não"
        ? d.descricaoSonora
        : "Não declarada";
      d.occurrences = _.map(d.occurrences, function(o) {
        if (o.space) {
          o.city = normalizeName(
            "city",
            o.space["En_Municipio"] || o.space["geoMunicipio"]
          );
          o.district = normalizeName(
            `district_${install.name}`,
            o.space["geoDistrito"]
          );
          delete o.space;
        }
        d.occurrenceCount = d.occurrences.length;
        return o;
      });
    }

    if (type == "project") {
      d.eventCount = d.events ? d.events.length : 0;
    }

    return d;
  });
}

/*
 * Import data
 */

function importData(install, type, data, doneImportData) {
  data = preprocessData(install, type, data);

  var collection = dbConnection.collection(`${install.name}-${type}s`);

  console.log(`Cleaning up...`);
  collection.remove({}, function(err, result) {
    if (err) return doneImportData(err);

    console.log("Inserting...");
    collection.insertMany(data, function(err, result) {
      if (err) return doneImportData(err);

      console.log(`Imported ${result.insertedCount} ${type}s.`);
      postprocessData(install, doneImportData);
    });
  });
}

/*
 * Post-process data
 */
function postprocessData(install, donePostprocessData) {
  console.log(`Post-processing ${install.name}...`);

  async.series(
    [
      function(doneEach) {
        // unwind languages in a specific collection
        dbConnection.collection(`${install.name}-agents`).aggregate(
          [
            { $match: { "terms.area": { $ne: [] } } },
            {
              $project: {
                _id: 0,
                district: 1,
                gender: 1,
                age: 1,
                agentType: "$type",
                language: "$terms.area",
                tag: "$terms.tag"
              }
            },
            { $unwind: "$language" },
            { $out: `${install.name}-agents-languages` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // unwind languages in a specific collection
        dbConnection.collection(`${install.name}-agents`).aggregate(
          [
            { $match: { "terms.tag": { $ne: [] } } },
            {
              $project: {
                _id: 0,
                district: 1,
                gender: 1,
                age: 1,
                agentType: "$type",
                tag: "$terms.tag"
              }
            },
            { $unwind: "$tag" },
            { $out: `${install.name}-agents-tags` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // unwind languages in a specific collection
        dbConnection.collection(`${install.name}-agents-languages`).aggregate(
          [
            {
              $match: {}
            },
            {
              $project: {
                _id: 0,
                language: 1,
                agentType: 1,
                gender: 1,
                tag: 1
              }
            },
            { $unwind: "$tag" },
            { $out: `${install.name}-agents-language-tags` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // aggregate users by userId in agents collection
        dbConnection.collection(`${install.name}-agents`).aggregate(
          [
            {
              $project: {
                _id: 1,
                id: 1,
                userId: 1,
                createdAt: 1,
                individuals: {
                  $cond: [{ $eq: ["$type", "Individual"] }, 1, 0]
                },
                collectives: {
                  $cond: [{ $eq: ["$type", "Coletivo"] }, 1, 0]
                }
              }
            },
            {
              $group: {
                _id: "$userId",
                createdAt: { $min: "$createdAt" },
                agents: { $push: "$_id" },
                agentsCount: { $sum: 1 },
                individualsCount: { $sum: "$individuals" },
                collectivesCount: { $sum: "$collectives" }
              }
            },
            { $out: `${install.name}-users` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // unwind areas in a specific collection
        dbConnection.collection(`${install.name}-spaces`).aggregate(
          [
            { $match: { "terms.area": { $ne: [] } } },
            {
              $project: {
                _id: 0,
                district: 1,
                spaceType: "$type",
                activity: "$terms.area",
                tag: "$terms.tag"
              }
            },
            { $unwind: "$activity" },
            { $out: `${install.name}-spaces-activities` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // unwind areas in a specific collection
        dbConnection.collection(`${install.name}-spaces-activities`).aggregate(
          [
            { $match: { "terms.area": { $ne: [] } } },
            {
              $project: {
                _id: 0,
                district: 1,
                spaceType: 1,
                activity: 1,
                tag: 1
              }
            },
            { $unwind: "$tag" },
            { $out: `${install.name}-spaces-activities-tags` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // this step denormalize space's owner

        // get collections
        const agentsCollection = dbConnection.collection(
          `${install.name}-agents`
        );
        const spacesCollection = dbConnection.collection(
          `${install.name}-spaces`
        );

        // get list of spaces
        spacesCollection
          .find({ owner: { $ne: null } }, { _id: 1, owner: 1 })
          .toArray(function(err, spaces) {
            // iterate over spaces
            async.eachSeries(
              spaces,
              function(space, doneEachSeries) {
                // get owner
                agentsCollection.findOne({ _id: space.owner }, function(
                  err,
                  owner
                ) {
                  if (err) return doneEachSeries(err);

                  // attach owner to space
                  spacesCollection.update(
                    { _id: space._id },
                    { $set: { ownerId: space.owner, owner: owner } },
                    doneEachSeries
                  );
                });
              },
              doneEach
            );
          });
      },
      function(doneEach) {
        // unwind accessibility of spaces
        dbConnection.collection(`${install.name}-spaces`).aggregate(
          [
            { $match: { acessibilidade_fisica: { $ne: [] } } },
            {
              $project: {
                _id: 0,
                district: 1,
                capacity: 1,
                city: 1,
                type: 1,
                acessibilidade_fisica: 1
              }
            },
            { $unwind: "$acessibilidade_fisica" },
            { $out: `${install.name}-spaces-accessibility` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // get collections
        const agentsCollection = dbConnection.collection(
          `${install.name}-agents`
        );
        const eventsCollection = dbConnection.collection(
          `${install.name}-events`
        );

        // get list of agentes
        eventsCollection
          .find({ owner: { $ne: null } }, { _id: 1, owner: 1 })
          .toArray(function(err, events) {
            // iterate over events
            async.eachSeries(
              events,
              function(event, doneEachSeries) {
                // get owner
                agentsCollection.findOne({ _id: event.owner }, function(
                  err,
                  owner
                ) {
                  if (err) return doneEachSeries(err);

                  owner = _.pick(owner, ["name", "type"]);

                  // attach owner to event
                  eventsCollection.update(
                    { _id: event._id },
                    { $set: { ownerId: event.owner, owner: owner } },
                    doneEachSeries
                  );
                });
              },
              doneEach
            );
          });
      },
      function(doneEach) {
        // unwind occurrences
        dbConnection.collection(`${install.name}-events`).aggregate(
          [
            { $match: { occurrences: { $ne: [] } } },
            {
              $project: {
                _id: 0,
                eventId: "$_id",
                ownerId: 1,
                owner: 1,
                descricaoSonora: 1,
                traducaoLibras: 1,
                terms: 1,
                occurrences: 1
              }
            },
            { $unwind: "$occurrences" },
            { $out: `${install.name}-occurrences` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // flatten occurrences
        dbConnection.collection(`${install.name}-occurrences`).aggregate(
          [
            { $match: { occurrences: { $ne: [] } } },
            {
              $project: {
                _id: 0,
                eventId: 1,
                ownerId: 1,
                owner: 1,
                descricaoSonora: 1,
                traducaoLibras: 1,
                spaceId: 1,
                city: "$occurrences.city",
                district: "$occurrences.district",
                language: "$terms.linguagem",
                tag: "$terms.tag",
                _id: "$occurrences.id",
                startsAt: "$occurrences.rule.startsAt",
                duration: "$occurrences.rule.duration",
                frequency: "$occurrences.rule.frequency",
                startsOn: "$occurrences.rule.startsOn",
                until: "$occurrences.rule.until",
                price: "$occurrences.rule.price",
                endsAt: "$occurrences.rule.endsAt",
                price: "$occurrences.rule.price",
                spaceId: "$occurrences.rule.spaceId"
              }
            },
            { $out: `${install.name}-occurrences` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // get collections
        const spacesCollection = dbConnection.collection(
          `${install.name}-spaces`
        );
        const occurrencesCollection = dbConnection.collection(
          `${install.name}-occurrences`
        );

        occurrencesCollection
          .find({ spaceId: { $ne: null } }, { _id: 1, spaceId: 1 })
          .toArray(function(err, occurrences) {
            async.eachSeries(
              occurrences,
              function(occurrence, doneEachSeries) {
                spacesCollection.findOne(
                  { _id: parseInt(occurrence.spaceId) },
                  function(err, space) {
                    if (err) return doneEachSeries(err);

                    space = _.pick(space, ["_id", "type", "name"]);

                    occurrencesCollection.update(
                      { _id: occurrence._id },
                      { $set: { space: space } },
                      doneEachSeries
                    );
                  }
                );
              },
              doneEach
            );
          });
      },
      function(doneEach) {
        // flatten occurrences
        dbConnection.collection(`${install.name}-occurrences`).aggregate(
          [
            { $match: { occurrences: { $ne: [] } } },
            {
              $project: {
                _id: 0,
                eventId: 1,
                ownerId: 1,
                owner: 1,
                descricaoSonora: 1,
                traducaoLibras: 1,
                space: 1,
                spaceId: 1,
                city: 1,
                district: 1,
                language: 1,
                tag: 1,
                startsAt: 1,
                duration: 1,
                frequency: 1,
                startsOn: 1,
                until: 1,
                price: 1,
                endsAt: 1,
                price: 1
              }
            },
            { $unwind: "$language" },
            { $out: `${install.name}-occurrences-languages` }
          ],
          doneEach
        );
      },
      function(doneEach) {
        // flatten occurrences
        dbConnection
          .collection(`${install.name}-occurrences-languages`)
          .aggregate(
            [
              { $match: { occurrences: { $ne: [] } } },
              {
                $project: {
                  _id: 0,
                  eventId: 1,
                  ownerId: 1,
                  owner: 1,
                  descricaoSonora: 1,
                  traducaoLibras: 1,
                  space: 1,
                  spaceId: 1,
                  city: 1,
                  district: 1,
                  language: 1,
                  tag: 1,
                  startsAt: 1,
                  duration: 1,
                  frequency: 1,
                  startsOn: 1,
                  until: 1,
                  price: 1,
                  endsAt: 1,
                  price: 1
                }
              },
              { $unwind: "$tag" },
              { $out: `${install.name}-occurrences-languages-tags` }
            ],
            doneEach
          );
      }
    ],
    donePostprocessData
  );
}
