<?php
// $ID$

include_once("wisski_ARC2.php");
/**
 * This class handles the basic communication between the ARC triplestore and 
 * the drupal wisski-Module.
 * @author: Mark Fichtner
 */ 

class wisski_ARCAdapter 
{
  
  public $config;
  public $graphName;
  
 /**
  * Get the erlangen-crm NS
  * This Function is outdated and will be removed in future versions.
  * @author: Mark Fichtner
  * @return: the ecrm-NS
  */
  public function wisski_ARCAdapter_getEcrmNS() {
    return "ecrm";
  }
  
 /**
  * Basic constructor for initialisation
  * @author: Mark Fichtner
  * @return: an array of namespaces 
  */ 
  public function __construct($newConfig, $newGraphName) {
    $this->config = $newConfig; 
    $this->graphName = $newGraphName;
  }  
    
 /**
  * Get a wisski-store object
  * @author: Mark Fichtner
  * @return: the wisski-store-object
  */
  public function wisski_ARCAdapter_getStore() {
    $myconfig = $this->config;
    $myconfig['store_write_buffer'] = 500;
    
    $this->config = $myconfig;
  
    include_once("wisski_ARC2.php");
    $store = new wisski_ARC2($this->config);
    if (!$store->isSetUp()) {
      $store->setUp();
    }
    
    return $store;
  }

 /**
  * Returns the basic graphname of the current store-object
  * @author: Mark Fichtner
  * @return: the graphname
  */  
  public function wisski_ARCAdapter_graphName() {
    return $this->graphName;
  }                                            
  

 /**
  * This function returns the namespaces for the project. These namespaces
  * should be generated from the ontology in the beginning and should be
  * supplemented during the project.
  * @author: Mark Fichtner
  * @return: an array of namespaces 
  */
  public function wisski_ARCAdapter_getNamespaces() {
    return variable_get("wisski_namespaces", array());
  }

 /**
  * Set the namespace-array to a given value
  * @author: Mark Fichtner
  */
  public function wisski_ARCAdapter_setNamespaces($array) {
    variable_set("wisski_namespaces", $array);
  }
  
 /**
  * Add an array of namespaces
  * @author: Mark Fichtner
  */
  public function wisski_ARCAdapter_addNamespaces($newarray) {
    $array = variable_get("wisski_namespaces",array());
    foreach($newarray as $key => $value) {
      $newkey = $key;
      $i = 0;
      
      // already there?
      if(in_array($value, $array))
        continue;
      
      // not there but key is there?  
      while(array_key_exists($newkey, $array)) {
        $newkey = $key . $i;
        $i++;
      }
      
      $array[$newkey] = $value;
    }
      
    variable_set("wisski_namespaces", $array);
  }


  

 /**
  * This function returns the object of a triple with given subject and 
  * predicate.
  * @author: Mark Fichtner
  * @return: An array of found object(s) of the triple or the empty array 
  * if none was found
  */   
  public function wisski_ARCAdapter_getObjForSubjPred($subj, $pred) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $store = $this->wisski_ARCAdapter_getStore();

    $subj = $this->wisski_ARCAdapter_delNamespace($subj);
    $pred = $this->wisski_ARCAdapter_delNamespace($pred);
    

    $obj = array();

    $q = 'SELECT ?o WHERE { <' . $subj .'> <' . $pred . '> ?o }';

    if ($rows = $store->query($q, 'rows')) {
      foreach ($rows as $row) {
			  $obj[] = $row['o'];
      }
    }
    
    return $obj;
  }

 /**
  * Get all triples from this store
  * @author: Mark Fichtner
  * @return an array of triples indexed by s, p and o
  */  
  public function wisski_ARCAdapter_getTriples() {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $q = 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }';
            
    $rows = array();
    
    $erg = $store->query($q, 'rows');
    
    if(!empty($erg))
      $rows = $erg;
  
    return $rows;
  }

 /**
  * Get the triples for a certain obj
  * @param obj a given object (url)
  * @author: Mark Fichtner
  * @return an array of triples indexed by s and p
  */
  public function wisski_ARCAdapter_getTriplesForObject($obj) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $obj = $this->wisski_ARCAdapter_delNamespace($obj);

    $q = "";

    if(strstr($obj, ":") != FALSE && strstr($obj, "http:") != FALSE) 
      $q = 'SELECT ?s ?p WHERE { ?s ?p <' . $obj .'> }';
    else 
      $q = 'SELECT ?s ?p WHERE { ?s ?p "' . $obj .'" }';
        
    $rows = array();

    $erg = $store->query($q, 'rows');
    
    if(!empty($erg))
      $rows = $erg;
  
    return $rows;
  }

  public function wisski_ARCAdapter_getLiteralsByRegex($regex) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $q = <<< END
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?lit
WHERE {
  { ?x skos:prefLabel ?lit }
  UNION
  { ?x skos:altLabel ?lit }
  FILTER (regex(?lit, " $regex ", "i") ||
          regex(?lit, "^$regex ", "i") ||
          regex(?lit, " $regex$", "i") ||
          regex(?lit, "^$regex$", "i") ||
         )
}
END;

    $rows = array();

print_r($q);
print_r("§§SQL");
$ts = microtime(TRUE);
print_r($store->query($q,'sql'));
print_r($store->getErrors());
$ts = microtime(TRUE) - $ts;
print_r("sql query time: $ts secs");
print_r("SQL§§"); 
print_r("§§INFO");
print_r($store->query($q,'infos'));
print_r("INFO§§");
$rus = getrusage();
$ts = $rus['ru_stime.tv_usec'];
$tu = $rus['ru_utime.tv_usec'];
    $erg = $store->query($q, 'rows');
$rus = getrusage();
$ts = $rus['ru_stime.tv_usec'] - $ts;
$tu = $rus['ru_utime.tv_usec'] - $tu;
print_r("query user time: $tu mysecs ");
print_r("query system time: $ts mysecs ");
    
    
    if(!empty($erg))
      $rows = $erg;
  
    return $rows;

  }

 /**
  * Delete all triples containing a given object
  * @param obj a given object (url)
  * @author: Mark Fichtner
  * @return an array of triples indexed by s and p
  */  
  public function wisski_ARCAdapter_delTriplesForObject($obj) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $obj = $this->wisski_ARCAdapter_delNamespace($obj);

    $q = "";

    if(strstr($obj, ":") != FALSE && strstr($obj, "http:") != FALSE) 
      $q .= 'DELETE FROM ' . $this->wisski_ARCAdapter_graphName() . ' WHERE { ?s ?p <' . $obj .'> }';
    else 
      $q .= 'DELETE FROM ' . $this->wisski_ARCAdapter_graphName() . ' WHERE { ?s ?p "' . $obj .'" }';
        
    $rows = array();
    
    $erg = $store->query($q, 'rows');
    
    if(!empty($erg))
      $rows = $erg;
  
    return $rows;
  }

 /**
  * Delete a given triple
  * @param subj a given subject (url)
  * @param pred a given predicate (url)
  * @param obj a given object (url)
  * @author: Mark Fichtner
  * @return an array of triples indexed by s, p and o
  */
  public function wisski_ARCAdapter_delTriple($subj, $pred, $obj) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $rows = $store->query("SELECT ?z WHERE { <$subj> <$pred> ?z }", 'rows');
    $rowOut;

    foreach($rows as $row) {
      if($row['z'] == $obj) {
        $rowOut = $row;
        break; 
      }
      if($row['z'] == $this->wisski_ARCAdapter_delNamespace($obj)) {
        $rowOut = $row;
        break;
      }
    }
    
    if(count($rowOut) == 0)
      return;

    $q = "";
    if($outRow['z type'] == "uri") 
      $q1 = $q . 'DELETE { <' . $subj . '> <'. $pred .'> <' . $rowOut['z'] .'> }';
    else
      $q1 = $q . 'DELETE { <' . $subj . '> <'. $pred .'> "' . $rowOut['z'] .'" }';

    $rows = array();
    
    $erg = $store->query($q1);

    if(!empty($erg))
      $rows = $erg;
  
    return $rows;
  }
    
 /**
  * Get all triples for a given subject
  * @param subj a given subject (url)
  * @author: Mark Fichtner
  * @return an array of triples indexed by p and o
  */  
  public function wisski_ARCAdapter_getTriplesForSubject($subj) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();

    $subj = $this->wisski_ARCAdapter_delNamespace($subj);

    $q = "";

    $q .= 'SELECT ?p ?o WHERE { <'. $subj . '> ?p ?o }';
  
    return $store->query($q, 'rows');
  }

 /**
  * Delete all triples for a given subject
  * @param subj a given subject (url)
  * @author: Mark Fichtner
  * @return an array of triples indexed by p and o
  */  
  public function wisski_ARCAdapter_delTriplesForSubject($subj) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();

    $store = $this->wisski_ARCAdapter_getStore();
    $subj = $this->wisski_ARCAdapter_delNamespace($subj);

    $q = "";

    $q .= 'DELETE FROM ' . $this->wisski_ARCAdapter_graphName() . ' WHERE { <'. $subj . '> ?p ?o }';
  
    return $store->query($q, 'rows');
  }

 /**
  * Get all concepts/classes from the ontology
  * @author: Mark Fichtner
  * @return an array of concepts (uri)
  */  
  public function wisski_ARCAdapter_getAllConcepts() {
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $store = $this->wisski_ARCAdapter_getStore();
    
    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    $q = $ns;
    $q .= 'SELECT ?s WHERE { ?s rdf:type owl:Class }';

    $out = array();

	  if ($rows = $store->query($q, 'rows')) {

      foreach ($rows as $row) {
        if($row['s type'] != "bnode") {
  			  if(!in_array($this->wisski_ARCAdapter_addNamespace($row['s']), $out)  )
	  		    $out[] = $this->wisski_ARCAdapter_addNamespace($row['s']);
        }
      }
    }  		
    sort($out);	
    return $out;
  }
    
 /**
  * Get all upper concepts for a given concept
  * @param classToWork the concept which is used as a starting point
  * @author: Mark Fichtner
  * @return an array of concepts (uri)
  */  
  public function wisski_ARCAdapter_getUpperClasses($classToWork) {
	  $namespaces = $this->wisski_ARCAdapter_getNamespaces();
	  $store = $this->wisski_ARCAdapter_getStore();

    $todo = array();
    
    $todo[] = $this->wisski_ARCAdapter_addNamespace($classToWork);

    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    for ($i = 0; $i < sizeof($todo); $i+=1) {
		  $class = $todo[$i];
		  $q = $ns;
		  if(substr($class, 0, 7) === "http://")
  		  $q .= 'SELECT ?o WHERE { <' . $class .'> rdfs:subClassOf ?o }';
		  else
		    $q .= 'SELECT ?o WHERE { ' . $class .' rdfs:subClassOf ?o }';

		  if ($rows = $store->query($q, 'rows')) {

        foreach ($rows as $row) {
				  if($row['o type'] != "bnode") {
					  if(in_array($this->wisski_ARCAdapter_addNamespace($row['o']), $todo) == FALSE)
						  $todo[] = $this->wisski_ARCAdapter_addNamespace($row['o']);
          }
        }
      }  			
    }

    return $todo;
  }

 /**
  * Get all upper properties for a given property
  * @param classToWork the property which is used as a starting point (uri)
  * @author: Mark Fichtner
  * @return an array of properties (uri)
  */  
  public function wisski_ARCAdapter_getUpperProperties($classToWork) {
	  $namespaces = $this->wisski_ARCAdapter_getNamespaces();
	  $store = $this->wisski_ARCAdapter_getStore();

    $todo = array();

    $todo[] = $this->wisski_ARCAdapter_addNamespace($classToWork);

    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    for ($i = 0; $i < sizeof($todo); $i+=1) {
		  $class = $todo[$i];
		  $q = $ns;
		  if(substr($class, 0, 7) === "http://")
		    $q .= 'SELECT ?o WHERE { <' . $class .'> rdfs:subClassOf ?o }';
      else
  		  $q .= 'SELECT ?o WHERE { ' . $class .' rdfs:subPropertyOf ?o }';

		  if ($rows = $store->query($q, 'rows')) {
        foreach ($rows as $row) {
				  if($row['o type'] != "bnode") {
					  if(in_array($this->wisski_ARCAdapter_addNamespace($row['o']), $todo) == FALSE)
						  $todo[] = $this->wisski_ARCAdapter_addNamespace($row['o']);
          }
        }
      }  			
    }

    return $todo;
  }

 /**
  * Get all subconcepts/classes for a given class
  * @param classToWork the concept which is used as a starting point (uri)
  * @author: Mark Fichtner
  * @return an array of concepts (uri)
  */  
  public function wisski_ARCAdapter_getSubClasses($classToWork) {
	  $namespaces = $this->wisski_ARCAdapter_getNamespaces();
	  $store = $this->wisski_ARCAdapter_getStore();

    $todo = array();

    $todo[] = $this->wisski_ARCAdapter_addNamespace($classToWork);

    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    for ($i = 0; $i < sizeof($todo); $i+=1) {
		  $class = $todo[$i];
		  $q = $ns;
		  $q .= 'SELECT ?s WHERE { ?s rdfs:subClassOf ' . $class .' }';

		  if ($rows = $store->query($q, 'rows')) {
        foreach ($rows as $row) {
				  if($row['s type'] == "uri") {
					  if(in_array($this->wisski_ARCAdapter_addNamespace($row['s']), $todo) == FALSE)
						  $todo[] = $this->wisski_ARCAdapter_addNamespace($row['s']);
          }
        }
      }  			
    }

    return $todo;
  }

 /**
  * Get all properties which have a given concept as their domain
  * @param domain the concept which is used as a starting point (uri)
  * @author: Mark Fichtner
  * @return an array of properties (uri)
  */
  public function wisski_ARCAdapter_getPropertiesForDomain($domain) {
 	  $namespaces = $this->wisski_ARCAdapter_getNamespaces();
 	  $store = $this->wisski_ARCAdapter_getStore();

 	  $properties = array(); 

 	  $ns = "";
 	  foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    $q = $ns;
    $q .= 'SELECT ?s WHERE { ?s rdfs:domain <' . $this->wisski_ARCAdapter_delNamespace($domain) .'> }';

    if ($rows = $store->query($q, 'rows')) {
      foreach ($rows as $row) {
        if($row['s type'] != "bnode") {
          if(in_array($this->wisski_ARCAdapter_addNamespace($row['s']), $properties) == FALSE)
            $properties[] = $this->wisski_ARCAdapter_addNamespace($row['s']);
        }
      }
    }
	
    return $properties;

  }

 /**
  * Store a given triple to the triple store
  * This function is obsolete and will be removed in future revisions!
  * @param triple a triple indexed somehow
  * @author: Mark Fichtner
  */    
  public function wisski_ARCAdapter_saveToARC($triple) {
    
	  $namespaces = $this->wisski_ARCAdapter_getNamespaces();
	  $store = $this->wisski_ARCAdapter_getStore();

	  $properties = array();

	  $ns = "";
	  foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    $q = $ns;
    $q .= 'INSERT INTO <' . $this->wisski_ARCAdapter_graphName() . '> { ';
		foreach ($triple as $t) {
		  $q .= $t;
		  $q .= ' ';
    }
    $q .= '. }';

	  $store->query($q);
  }
  
 /**
  * Get all domain concepts for a given property
  * @param pred the property (uri)
  * @author: Mark Fichtner
  * @return an array of concepts (uri)
  */
  public function wisski_ARCAdapter_getDomainConcepts($pred) {
  	$namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $store = $this->wisski_ARCAdapter_getStore();
    $q = "";
    foreach ($namespaces as $name => $val) {
      $q .= "PREFIX $name:\t<$val>\n";
    }	

    $q .= "SELECT ?z WHERE { $pred rdfs:domain ?z . ?z rdf:type owl:Class }";

  	if ($rows = $store->query($q, 'rows')) {
  		if ( count($rows) != 0) {
  			$ret = array();
			
  			foreach ($rows as $row) {
  				if($row['z type'] != "bnode")
  					$ret[] = $row['z'];
  			}

  			if(count($ret) > 0) 
  				return $ret;			

  		}
  	}
	
  	$namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

  	$q = $ns . "SELECT ?z WHERE { $pred rdfs:domain ?x . ?x owl:unionOf ?z }"; 

  	$x = "";

  	if ($rows = $store->query($q, 'rows')) {
  		if ( count($rows) == 0 )
  			return array();
  	}

	
  	$qStart = $ns . "SELECT ";
  	$qMid = "WHERE { $pred rdfs:domain ?x . ?x owl:unionOf ?z0 ";
  	$qEnd = "}";

	
  	for($i=0; ; $i++) {
		
  		$qStart .= ("?x" . $i . " ?z" . ($i + 1) . " ");
  		$qMid .= (". ?z" . $i . " rdf:first ?x" . $i . " . ?z" . $i . " rdf:rest ?z" . ($i + 1) . " ");
		
  		$rows = $store->query(($qStart . $qMid . $qEnd), 'rows');
		
  		if(count($rows) > 1) {
  			drupal_set_message("Too many results for the property " . $pred . ", perhaps wrong domain?", "error");
        drupal_set_message("Query was: " . serialize(($qStart . $qMid . $qEnd)));
	  		break;
	  	}

	  	if(count($rows) == 0) {
	  	  $qnew = "SELECT ?x0 ?x1 WHERE { <" . $this->wisski_ARCAdapter_delNamespace($pred) . "> <" . $this->wisski_ARCAdapter_delNamespace('rdfs:domain') . 
	  	  "> _:1 . _:1 <" . $this->wisski_ARCAdapter_delNamespace('owl:unionOf') . "> _:2 . _:2 <" 
	  	  . $this->wisski_ARCAdapter_delNamespace('rdf:first') . "> ?x0 . _:2 <" 
	  	  . $this->wisski_ARCAdapter_delNamespace('rdf:rest') ."> _:3 . _:3 . <" 
	  	  . $this->wisski_ARCAdapter_delNamespace('rdf:first') . "> ?x1 . }";
	  		drupal_set_message("Property analysis failed at $qStart . $qMid . $qEnd. No Domain!", "error");
	  		break;
	  	}

  		if($rows[0]['z' . ($i + 1) . ' type'] != "bnode") {
  			$ret = array();
  			for($ii = 0; $ii <= $i; $ii++) {
  				$ret[] = $rows[0]['x' . $ii . '']; 
  			}
  			return $ret;
  		}
  	}
	  return array();	
	}

 /**
  * Get all datatype properties for a given concept
  * @param concept the concept used as a starting point (uri)
  * @author: Mark Fichtner
  * @return an array of datatype properties (uri)
  */
  public function wisski_ARCAdapter_getDatatypePropertiesForConcept($concept) {
    
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $store = $this->wisski_ARCAdapter_getStore();
            
    $properties = array();
                
    $ns = "";
    foreach ($namespaces as $name => $val) {
      $ns .= "PREFIX $name:\t<$val>\n";
    }

    $query = $ns . "SELECT ?x WHERE { ?x rdf:type owl:DatatypeProperty }";
    
    $properties = array();
    $outprop = array();
    
    if ($rows = $store->query($query, 'rows')) {
      foreach($rows as $row) {
        if($row['x type'] != "bnode") {
          if(in_array($this->wisski_ARCAdapter_addNamespace($row['x']), $properties) == FALSE)
            $properties[] = $this->wisski_ARCAdapter_addNamespace($row['x']);
        }
      }
    }
    
    foreach($properties as $property) {
      $domainConcepts = $this->wisski_ARCAdapter_getDomainConcepts($property);
      $allConcepts = array();
      foreach($domainConcepts as $domainConcept) {
        $allConcepts = array_merge($allConcepts, $this->wisski_ARCAdapter_getSubClasses($domainConcept));
      }
    
      if(in_array($this->wisski_ARCAdapter_addNamespace($concept), $allConcepts))
        $outprop[] = $this->wisski_ARCAdapter_delNamespace($property);
    }

    return $outprop;
  }
  
 /**
  * A supporting function to extract namespaces from complex strings
  * @author: Mark Fichtner
  * @return: Returns the inputstring with the replaced namespace
  */
  public function wisski_ARCAdapter_addNamespace($string) {
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $outstring = $string;
    foreach($namespaces as $name => $val) {
      $outstring = str_replace($val, $name.':', $outstring);
    }
    
    $outstring = str_replace(url('<front>',array('absolute' => TRUE)) . 'content/', '', $outstring); 
    return $outstring;
  }

  /**
  * A supporting function to build complex uris from namespaces 
  * @author: Mark Fichtner
  * @return: Returns the inputstring with the replaced namespace
  */  
  public function wisski_ARCAdapter_delNamespace($string) {
    $namespaces = $this->wisski_ARCAdapter_getNamespaces();
    $outstring = $string;
    
    foreach($namespaces as $name => $val) {
      $outstring = preg_replace('/^' . $name . ':/', $val, $outstring);
    }
    
    // same reversed
    $url = parse_url($outstring);
    if($url['scheme'] == NULL && $url['host'] == NULL) {
      $newurl = url('<front>',array('absolute' => TRUE));
      $outstring = substr($newurl, 0, strlen($newurl)-1) . '/content/' .  $outstring;
    }    
    
    return $outstring;
  }   
} 
  
  
  
  
  
  
  
  
  
  
