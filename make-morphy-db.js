var fs = require('fs');
var path = require('path');

var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database(path.join(__dirname, 'morphy.sqlite'));

var XmlStream = require('xml-stream');
var stream = fs.createReadStream(path.join(__dirname, 'morphy.xml'));
var encoding = 'utf8';


function main() {
    prepareDatabase();
}


function quit() {
    console.log("Done.");
    db.close();
}


function prepareDatabase() {
    db.serialize(function() {
        db.run("DROP TABLE IF EXISTS 'words';");
        db.run("CREATE TABLE 'words' ('id' INTEGER PRIMARY KEY AUTOINCREMENT,'wort' TEXT,'lemma' TEXT,'abl' TEXT,'art' TEXT,'der' TEXT,'form' TEXT,'gebrauch' TEXT,'gen' TEXT,'kas' TEXT,'komp' TEXT,'konj' TEXT,'mod' TEXT,'num' TEXT,'pers' TEXT,'perstyp' TEXT,'rekt' TEXT,'stellung' TEXT,'typ' TEXT,'wkl' TEXT,'zerlegung' TEXT);", function() {
            parseXML();
        });
    });
}


function parseXML() {

    var xml = new XmlStream(stream, encoding);

    xml.on('end', (item) => {
        quit();
    })

    xml.collect('lemma')
    xml.on('endElement: item', (item) => {

        xml.pause();

        var wort = item.form;
        // console.log(wort);

        db.run("BEGIN;")
        item.lemma.forEach(function(element, index, array) {
            var info = element.$
            var lemma = element.$text

            db.run('INSERT INTO "words" ("wort", "lemma", "abl", "art", "der", "form", "gebrauch", "gen", "kas", "komp", "konj", "mod", "num", "pers", "perstyp", "rekt", "stellung", "typ", "wkl", "zerlegung") VALUES ($1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text, $9::text, $10::text, $11::text, $12::text, $13::text, $14::text, $15::text, $16::text, $17::text, $18::text, $19::text, $20::text);', [wort, lemma, info['abl'], info['art'], info['der'], info['form'], info['gebrauch'], info['gen'], info['kas'], info['komp'], info['konj'], info['mod'], info['num'], info['pers'], info['perstyp'], info['rekt'], info['stellung'], info['typ'], info['wkl'], info['zerlegung']], (err) => {
                if (err) {
                    console.log('error running query', err);
                    process.exit()
                }
            });
        })

        db.run("COMMIT;", () => {
            xml.resume();
        })

    })
}

main();
