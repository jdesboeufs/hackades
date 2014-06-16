var _ = require('lodash');
var mongodb = require('mongodb');

var client = mongodb.MongoClient;
var connectionString = 'mongodb://localhost/onema-hackathon';

var communes = require('./data/communes.json');
var communesByInsee = {};
_.forEach(communes.features, function(commune) {
    communesByInsee[commune.properties['NUM_COM']] = commune.geometry;
});

client.connect(connectionString, function(err, db) {
    if (err) console.log(err);

    var collection = db.collection('prelevements');
    collection.aggregate([
        { $match: { 'properties.analysisStatus': 1 }},
        { $group: { _id: '$properties.bssInseeCode', average: { $avg: '$properties.analysisResult' } } }
    ], function(err, results) {
        if (err) return console.log(err);

        var output = {
            type: 'FeatureCollection',
            crs: { type: "name", properties: { name: 'urn:ogc:def:crs:OGC:1.3:CRS84' } },
            features: _.map(results, function(result) {
                var options = { color: 'Black', weight: '1' };
                if (result.average < 25) options.fillColor = 'DarkGreen';
                if (result.average >= 25 && result.average <= 50) options.fillColor = 'DarkOrange';
                if (result.average > 50) options.fillColor = 'DarkRed';
                return {
                    type: "Feature",
                    properties: {
                        insee: result._id,
                        value: result.average,
                        _storage_options: options
                    },
                    geometry: communesByInsee[result._id]
                };
            })
        };
     
        console.log(JSON.stringify(output));
        process.exit();
    });

});
