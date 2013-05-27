<?php

include_once("wisski_ARCAdapter.php");

/**
 * This class comprises the functionality for the wisski.module
 * It extends the wisski_ARCAdapter thus its constructor needs the 
 * configuration for the triplestore of the wisski-ontology and a
 * suitable name for the graph contained in the triplestore (usually
 * the same as the store's name 
 * @author Mark Fichtner
 */
class wisski_Store extends wisski_ARCAdapter {
	
 /**
	* Get a new unique id
	* 
	* @author: Mark Fichtner
	* @return: Returns a system-local unique id
	*/
	public function wisski_Store_getNewUniqueID() {
    drupal_load('module', 'wisski');
    return wisski_get_uid();
	}
	
 /**
	* Delete all data from WissKI
	* 
	* @author: Mark Fichtner
	*/
	public function wisski_Store_clearAll() {
		// drop the database
		$this->wisski_ARCAdapter_getStore()->drop();

		// delete the namespaces
		variable_set("wisski_namespaces", array());
		$sql = "SELECT nid FROM {node} WHERE type = 'class' OR type = 'property' OR type = 'individual'";
		$result = db_query($sql);

		// delete all nodes
		while ($row = db_fetch_object($result)) {
			node_delete($row->nid);
		}
	}

 /**
	* A supporting public function for building the triple-set. These triples
	* are supported by arc2. Modes for the tripleset can be 0 (Only incoming),
	* 1 (Only outgoing) and 2 (Both).
	* @param: $node	A drupal node, $mode	the mode of the triple set
	* @author: Mark Fichtner
	* @return: Returns the triple set representing $node
	*/
	public function wisski_Store_buildTriples($node, $mode = '2') {
   	/* Store all triples for image-generation
   	* triples have to be an array consisting of:
   	* "s": the subject, "s_type": the type: "uri", "bnode" or "var"
   	* "p": the predicate,
   	* "o": the object, "o_type": "uri", "bnode", "literal", or "var"
   	* "o_datatype": a datatype, "o_lang": the language e.g. "en-us"
   	*/   
   	$triples = array();
  
   	if($mode == 0 || $mode == 2) {
			if ($rows = $this->wisski_ARCAdapter_getTriplesForObject($node->title)) {
  	  	foreach ($rows as $row) {
					$triples[] = array(
											's'					 => $row['s'], 
											's_type' 		 => $row['s type'], 
											'p' 				 => $row['p'], 
											'o' 				 => $this->wisski_ARCAdapter_delNamespace($node->title), 
											'o_type' 		 => 'uri', 
											'o_datatype' => '', 
											'o_lang' 		 => '');
				}
			}
		}

		if($mode == 1 || $mode == 2) {
			if ($rows = $this->wisski_ARCAdapter_getTriplesForSubject($node->title)) {
  	  	foreach ($rows as $row) {
    			$triples[] = array(
											's' 				 => $this->wisski_ARCAdapter_delNamespace($node->title), 
											's_type' 		 => 'uri', 
											'p' 				 => $row['p'], 
											'o' 				 => $row['o'], 
											'o_type' 		 => $row['o type'], 
											'o_datatype' => $row['o datatype'], 
											'o_lang' 		 => $row['o lang']);
				}
			}
		}

		return $triples;
	}

 /**
 	* This public function returns the ontology uris in the store
 	* @author: Mark Fichtner
 	* @return: Returns an array of ontology uris or FALSE if none was found
 	*/
	public function wisski_Store_getOntologyURI() {
		$store = $this->wisski_ARCAdapter_getStore();
		$q = "SELECT * WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Ontology> . } ";
		$result = $store->query($q);

		if(!$result['result'])
			return FALSE;
			
		$out = array();
		foreach($result['result']['rows'] as $row) {
			$out[] = $row['x'];
		}
		return $out;
	}
	
 /**
	* This public function is used to delete the ontology in the triple store
	* @author: Mark Fichtner
	* @return: Returns true if something was deleted
	*/
	public function wisski_Store_delOntology($form, &$form_state) {
	 	$uris = $this->wisski_Store_getOntologyURI();
	 	drupal_set_message(serialize($uris));
	 	
	 	return;

	 	foreach($uris as $uri) {
			$query = "DELETE FROM <$uri> { ?s ?p ?o } WHERE { ?s ?p ?o . }";
		
			$store = $this->wisski_ARCAdapter_getStore();
			$store->query($query);
		}
		
		$sql = "SELECT nid FROM {node} WHERE type = 'class' OR type = 'property'";
		$result = db_query($sql);
		while ($row = db_fetch_object($result)) {
			node_delete($row->nid);
		}
		
		variable_set("wisski_namespaces", array());
				
		return TRUE;		
		
	}	
	
 /**
	* This public function is used to delete the data in the triple store
	* @author: Mark Fichtner
	* @return: Returns true if something was deleted
	*/
	public function wisski_Store_delData($form, &$form_state) {
	
		global $base_url;

		//$query = "DELETE * WHERE { GRAPH <$base_url> { ?s ?p ?o . } }";
		$query = "DELETE FROM <$base_url> { ?s ?p ?o } WHERE { ?s ?p ?o . }";
		
		$store = $this->wisski_ARCAdapter_getStore();
		$store->query($query, "rows");
		
		$sql = "SELECT nid FROM {node} WHERE type = 'individual'";
		$result = db_query($sql);
		while ($row = db_fetch_object($result)) {
			node_delete($row->nid);
		}
		
		variable_set("wisski_data",array());
				
		return TRUE;		
		
	}
	
 /**
	* This public function is used to dump data of the triple store in RDF/OWL-Format
	* @author: Mark Fichtner
	* @return: Returns true if something was dumped
	*/
	public function wisski_Store_dumpData($form, &$form_state) {
	
		global $base_url;
		$query = "SELECT * WHERE { GRAPH <$base_url> { ?s ?p ?o . } }";
	
		$store = $this->wisski_ARCAdapter_getStore();
		$rows = $store->query($query, 'rows');
	
		if(!$rows) {
			drupal_set_message("No data for dumping.", "error");
			return FALSE;
		}	
		$doc = $store->toRDFXML($rows);

		if(!file_check_directory(file_directory_path())) {
			drupal_set_message("The directory for files is not accessible.", "error");
			return FALSE;
		}	
		$filename = file_save_data($doc, $filename);
		$dumps = variable_get("wisski_dumps", array());
		$dumps[] = array('file' => $filename, 'size' => filesize($filename), 'date' => date('Y-m-d H:i:s'));
		variable_set("wisski_dumps", $dumps);

		return TRUE;
	
	}

 /**
	* This public function is used to import data to the triple store in RDF/OWL-Format
	* @author: Mark Fichtner
	* @return: Returns true if something was imported
	*/
	public function wisski_Store_readData($form, &$form_state) {
		global $base_url;
		$datafile = $form_state['values']['wisski_data'];
		$parser = ARC2::getRDFParser();
		$store = $this->wisski_ARCAdapter_getStore();
		$datafiles = variable_get("wisski_data", array());
		
		$parser->parse($datafile);

		// parse to get the triples		
		$triples = $parser->getTriples();
		
		if(!$triples || $errors = $store->getErrors()) {
			drupal_set_message("Errors parsing datafile " . $datafile . ": " . serialize($errors), "error");
			return FALSE;
		}
		
		// add nodes
		foreach($triples as $triple) {
			wisski_store_addNodeForTriple($triple);			
		}
		
		// store the triples
		$store->insert($triples, $base_url);		
		if($errors = $store->getErrors()) {
			drupal_set_message("Errors loading datafile " . $datafile . ": " . serialize($errors), "error");
			return FALSE;
		} else {
			drupal_set_message("Succesfully loaded $datafile");
			$datafiles = variable_get("wisski_data", array());
			if(!in_array($datafile, $datafiles))
				$datafiles[] = $datafile;
			variable_set("wisski_data", $datafiles);
			return TRUE;
		}
		
	}

 /**
	* This public function is used to process the administration-values after
	* saving them
	* @author: Mark Fichtner
	* @return: Returns TRUE if an ontology was imported else FALSE
	*/
	public function wisski_Store_readOntology($form, &$form_state) {

  	$ontology = $form_state['values']['wisski_ontology'];

    $store = $this->wisski_ARCAdapter_getStore();

    $q = "ASK { <$ontology> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Ontology> . } ";

    $result = $store->query($q);

		if(!$result['result']) {
  		drupal_set_message('Import of new ontology in progress.');

			$this->wisski_Store_loadCompleteOnto($ontology, $ontology, $store);
		
			$this->wisski_Store_makeNodes();

			variable_set("wisski_ontology", $ontology);
			return TRUE;
		} else {
			drupal_set_message('This ontology is already loaded. Skipping.', 'error');
			return FALSE;
		}
	}
	
 /**
	* Generate nodes for all data in the triplestore
	* @author: Mark Fichtner
	*/
	public function wisski_Store_makeNodes() {

		$store = $this->wisski_ARCAdapter_getStore();
		$namespaces = $this->wisski_ARCAdapter_getNamespaces();
	  $q = "";
  	foreach ($namespaces as $name => $val) {
      $q .= "PREFIX $name:\t<$val>\n";
  	}

	  $q .= "SELECT ?x ?y WHERE { ?x rdf:type ?y }";
	  
	  $stored = array();

		if ($rows = $store->query($q, 'rows')) {
      foreach ($rows as $row) {
      	if(in_array($row['x'], $stored))
      		continue;

				$node = new stdClass();

				$name = $this->wisski_ARCAdapter_addNamespace($row['x']);
				$what = $this->wisski_ARCAdapter_addNamespace($row['y']);

				if ($row['x type'] != "uri")
					continue;

				if ($what == "owl:Ontology")
					continue;

				if ($what == "owl:Class")
					$node->type = 'class';
				elseif ($what == "owl:DatatypeProperty" || $what == "owl:ObjectProperty" || 
								$what == "owl:TransitiveProperty" || $what == "owl:SymmetricProperty" ||
								$what == "owl:InverseFunctionalProperty" || $what == "owl:FunctionalProperty")
					$node->type = 'property';
				else 
					$node->type = 'individual'; 
				$node->title = $name;
				$node->body = '';
				$node->teaser = '';
				$node->uid = 1;
				$node->status = 1;
				$node->promote = 1;

				node_save($node);
				
				if(module_exists("path"))
				  path_set_alias("node/" . $node->nid, "content/" . wisski_store_makePathTitle($node->title));
				
				$stored[] = $row['x'];                  
			}
		}
	}

 /**
	* Generate nodes for all data in the triplestore
	* @param newontology url for the ontology
	* @param ontologyname graph-name for the data of the ontology
	* @param store link to the store
	* @author: Mark Fichtner
	*/
	private function wisski_Store_loadCompleteOnto($newontology, $ontologyname, &$store) {
		drupal_set_message("loading ontology $newontology ");
		$store->query("LOAD <$newontology> INTO <$ontologyname>");
		if($errors = $store->getErrors()) {
			drupal_set_message("Errors loading Ontology " . $newontology . ": " . serialize($errors), "error");
		}
		
		$resource = fopen($newontology, 'r');
		
		$contents = stream_get_contents($resource);
		
		$contents = preg_replace("/\t|\r|\n/", " ", $contents);
		
		fclose($resource);
				
		preg_match('/<rdf:RDF[^>]*>/i', $contents, $nse);
				
		preg_match_all('/xmlns:[^=]*="[^"]*"/i', $nse[0], $nsarray);
		
		$ns = array();
		$toStore = array();
		foreach($nsarray[0] as $newns) {
		
			preg_match('/xmlns:[^=]*=/', $newns, $front);
			$front = substr($front[0], 6, strlen($front[0])-7);
			preg_match('/"[^"]*"/', $newns, $end);
			$end = substr($end[0], 1, strlen($end[0])-2);

			$ns[$front] = $end;
		}
		
		//Get the xmlns=-tag as base
		preg_match_all('/xmlns="[^"]*"/i', $nse[0], $toStore);
		
		foreach($toStore[0] as $itemGot) {
			$i=0;
			$key = 'base';
			
			preg_match('/"[^"]*"/', $itemGot, $item);
			$item	= substr($item[0], 1, strlen($item[0])-2);
			
			if(!array_key_exists($key, $ns)) {
				if(substr($item, strlen($item)-1, 1) != '#')
					$ns[$key] = $item . '#';
				else
					$ns[$key] = $item;
			}
			else {
				$newkey = $key . $i;
				while(array_key_exists($newkey, $ns)) {
					$i++;
					$newkey = $key . $i;
				}
				if(substr($item, strlen($item)-1, 1) != '#')
					$ns[$newkey] = $item . '#';
				else
					$ns[$newkey] = $item;
			}
		}
		
		$this->wisski_ARCAdapter_addNamespaces($ns);
		
		preg_match("/<owl:Ontology.*?<\/owl:Ontology>/i", $contents, $imports);
		
		preg_match_all("/<owl:imports.*?\/>/i", $imports[0], $tmpimp);
		
		foreach($tmpimp[0] as $import) {
			preg_match('/"[^"]*"/',$import, $anImport);
			$realimport = substr($anImport[0], 1, strlen($anImport[0])-2);
			$this->wisski_Store_loadCompleteOnto($realimport, $realimport, $store);
		}
		
	}
}  
