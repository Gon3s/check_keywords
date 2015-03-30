var elasticsearch = require('elasticsearch'),
	fs            = require('fs'),
	config        = require('./config.json'),
	Q             = require('q'),
	Spreadsheet   = require('edit-google-spreadsheet'),

	date           = new Date(), 
	Keywords       = [],
	KeywordsFailed = [],
	dateNow        = date.toISOString(),
	client;

/**
 * Fonction d'initialisation :
 *     - Chargement des mots clés depuis le fichier texte
 *     - Connexion à Elasticsearch
 *     - Connexion à Google Spreadsheet
 */
function Init()
{
	//== Initialisation du timer
	console.time('Init');

	Q.all([
		loadKeywordsFile(Keywords),        //== Ouverture et traitement des mots clés
		connectElasticsearch(),      //== Connection à Elasticsearch
	])
	.catch(function(err) {
		console.log(('Error init : ' + err).red);
		process.exit(0);
	})
	.done(function() {
		createScraping();
	});
}

/**
 * Chargement du fichier de mots clés.
 * @return {promise} La promesse que le fichier sera chargé.
 */
function loadKeywordsFile(Keywords)
{
	var deferred = Q.defer();

	console.log(('Load keywords file ...' + config.keywordsFile).grey);

	fs.readFile(config.keywordsFile, function (err, data) {
		if(err)
		{
			return deferred.reject('Can\'t load keywords file ' + config.keywordsFile);
		}

		var dataKeywords = data.toString("UTF-8").split("\n"),
			lenDataKeywords = dataKeywords.length,
			kwd, tmpKwd;

		for (var iDK = 0 ; iDK < lenDataKeywords ; ++iDK)
		{
			kwd = dataKeywords[iDK];
			tmpKwd = kwd.split("\t");
			if(tmpKwd.length == 2) Keywords.push({'keyword': tmpKwd[0].trim(), 'groupe': tmpKwd[1].trim(), 'try': 1});
		}

		console.log(('Load keywords file - ' + Keywords.length + ' mots clés').green);

		deferred.resolve();
	});

	return deferred.promise;
}

/**
 * Connexion à Elasticsearch
 * @return {promise} La promesse que la connexion sera effective.
 */
function connectElasticsearch()
{
	var deferred = Q.defer();

	console.log('Logging into Elasticsearch ...'.grey);

	client = new elasticsearch.Client({
		host: config.HostElasticsearch + ':' + config.PortElasticsearch
	});

	deferred.resolve();

	console.log('Logged into Elasticsearch'.green);

	return deferred.promise;
}

function createScraping()
{
	var spreadsheetId = config.spreadsheetId,
		len = spreadsheetId.length,
		the_promises = [];

	for (var i = 0 ; i < len ; ++i)
	{
		the_promises.push(new Scrap(spreadsheetId[i]));
	}

	Q.all(the_promises).done(function() {
		console.timeEnd('Init');
		console.log('Finish'.green);
		console.log(KeywordsFailed);
		process.exit(0);
	});
}

function Scrap(spreadsheetId)
{
	this.spreadsheetId = spreadsheetId;
	this.deferred = Q.defer();

	console.log(('Creation d\'un Scrap ' + this.spreadsheetId).green);

	this.init();

	return this.deferred.promise;
}

Scrap.prototype.init = function()
{
	var self = this;

	this.loadSpreadsheet().done(function() {
		self.startScraping();
	});
};

/**
 * Chargement de la Google spreadsheet.
 * @return {promise} La promesse que la feuille de calcul sera chargée.
 */
Scrap.prototype.loadSpreadsheet = function()
{
	var deferred = Q.defer(),
		self = this;

	Spreadsheet.load({
		debug: config.debug,
		spreadsheetId: self.spreadsheetId,
		worksheetId: config.worksheetId,
		oauth : {
			email: config.email,
			keyFile: config.keyFile
		}
	}, function sheetReady(err, spreadsheet) {
		if(err)
		{
			return deferred.reject(err);
		}

		self.ScraperSpreadsheet = spreadsheet;

		deferred.resolve();
	});

	return deferred.promise;
};

/**
 * Fonction qui demarre le scraping, fonction recursive qui se stoppe
 * lorsque le tableau de mot clé est vide
 */
Scrap.prototype.startScraping = function()
{
	if(Keywords.length == 0)
	{
		return this.deferred.resolve();
	}

	this.tmpKeyword = Keywords.shift();

	var self = this,
		keyword = self.tmpKeyword.keyword,
		groupe = self.tmpKeyword.groupe;
		keywordTry = self.tmpKeyword.try;

	console.log(('Scraping', keyword).grey);

	Q.delay(config.request_delay)
	.then(function() {
		return self.scrap();
	})
	.then(function() {
		self.startScraping();
	})
	.catch(function(err) {
		console.log((err).red);
		if(keywordTry != config.keywordTry)
			Keywords.push({'keyword': keyword, 'groupe': groupe, 'try': keywordTry + 1});
		else
			KeywordsFailed.push(keyword);
		self.startScraping();
	});
};

/**
 * Met à jour la fonction IMPORTXML dans la feuille de calcul
 * et récupère les résultats.
 * @param  {string} keyword Un mot clé.
 */
Scrap.prototype.scrap = function()
{
	var deferred = Q.defer(),
		start = 0,
		self = this,
		keyword = self.tmpKeyword.keyword;

	// Préparation des modifications des cellules.
	self.ScraperSpreadsheet.add({
		1: {
			1: "=IMPORTXML(\"" + config.google_domain + "/search?q=" + keyword + "&num=" + config.google_results + "&start=" + start + "\"; \"" + config.google_xpath_title + "\")",

			2: "=IMPORTXML(\"" + config.google_domain + "/search?q=" + keyword + "&num=" + config.google_results + "&start=" + start + "\"; \"" + config.google_xpath_url + "\")"
		}
	});

	// Applique les modifications.
	self.ScraperSpreadsheet.send(function(err) {
		if(err)
		{
			return deferred.reject(err);
		}

		self.receive(deferred);
	});

	return deferred.promise;
}

/**
 * Lecture des données de la feuille de calcul.
 * Si la fonction IMPORTXML est encore en cours d'analyse,
 * on relance une lecture des données.
 * @param  {deferred} deferred L'objet différé gérant l'analyse d'un mot clé.
 * @param  {string} keyword Un mot clé.
 * @param  {number} page    Le numéro de la page Google en cours de traitement.
 */
Scrap.prototype.receive = function(deferred)
{
	var self = this;

	Q.delay(config.receive_delay).then(function() {
		// L'option getValues permet d'avoir les véritables valeurs qui sont derrière les formules.
		// (Et non pas la formule contenu dans la cellule)
		self.ScraperSpreadsheet.receive({getValues: true}, function(err, rows, info) {
			if(err)
			{
				return deferred.reject(err);
			}

			// Je n'ai pas trouvé d'autre moyen de savoir si la fonction IMPORTXML
			// est encore en train de tourner, que de vérifier le contenu de la cellule
			// voir si la valeur est égale à "Chargement...".
			// Le problème c'est que ce texte doit dépendre de la langue du compte utilisé.
			// Et peut changer du jour au lendemain en fonction de Google.
			// Tester si la deuxième ligne est vide est également une mauvaise solution.
			// Car si pour une raison ou une autre, la ligne est vide,
			// on entre dans une boucle infinie.
			// Et acuellement si pour une raison inconnue la fonctione reste figée sur "Chargement...",
			// on entre également dans une boucle infinie.
			// TODO: Ce point mérite d'être amélioré.
			if(rows[1][1] == config.google_loading_text || rows[1][2] == config.google_loading_text)
			{
				console.log("Loading...".orange);
				self.receive(deferred);
			}
			else if(rows[1][1] == "#N/A" || rows[1][2] == "#N/A")
			{
				deferred.reject("#N/A");
			}
			else
			{
				// On peut maintenant traiter les données de manière asynchrone.
				// Donc on indique que la requête est terminée et qu'on peut
				// passer au mot clé suivant (ou page suivante).
				// Pendant ce temps là, on sauvegardera les données (dans elasticsearch).				
				self.processData(rows, deferred).done(function() {
					console.log(('Enregistrement ' + self.tmpKeyword.keyword).green);
					deferred.resolve();
				});
			}
		});
	});
}

/**
 * Traitement des lignes récupérées dans la feuille de calcul.
 * @param  {object} rows    Les lignes récupérées.
 * @param  {string} keyword Un mot clé.
 * @param  {number} page    Le numéro de la page Google en cours de traitement.
 */
Scrap.prototype.processData = function(rows, deferred)
{
	var regUrl = /\?q=(.+)/,
		regDomain = /https?:\/\/(.[^\/]+)/,
		start = 0,
		row,
		column,
		the_promises = [],
		self = this;

	for(row in rows)
	{
		// Il n'y a plus de données.
		if(rows[row][2] === undefined)
		{
			break;
		}

		// On ne traite pas les résultats dont l'URL
		// ne commence pas par "/url?".
		// Car il s'agit certainement d'un service de Google.
		// Comme Google Images ou Google News par exemple.
		if(rows[row][2].indexOf('/url?') != 0)
		{
			continue;
		}

		var matchUrl = regDomain.exec(rows[row][2]);

		the_promises.push(
			self.save({
				keyword: self.tmpKeyword.keyword,
				groupe: self.tmpKeyword.groupe,
				date: dateNow,
				url: (matchUrl !== null && matchUrl.length > 0)?regDomain.exec(rows[row][2])[0]:"",
				ancre: rows[row][1],
				position: start++
			})
		);
	}

	return Q.all(the_promises);
}

/**
 * Sauvegarde des données.
 * @param  {objet} data Les données à sauvegarder.
 */
Scrap.prototype.save = function(data)
{
	var deferred = Q.defer();

	client.index({
		index: config.IndexElasticsearch,
		type: config.TypeElasticsearch,
		body: data
	}, function (error, response) {
		if(error !== undefined)
		{
			deferred.reject(('Enregistrement impossible ' + error).red);
		}
		else
		{
			// console.log(('Enregistrement ' + data.keyword + ' - ' + data.url).green);
			deferred.resolve();
		}
	});

	return deferred.promise;
}

Init();
