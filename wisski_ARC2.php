<?php
/** The m_ARC-Interface for SPARQL-Query Enhancing
 * @author Mark Fichtner
 **/

include_once("arc/store/ARC2_Store.php");
include_once("arc/store/ARC2_RemoteSPARQLOneDotOneStore.php");

class wisski_ARC2 extends ARC2_Store {
  

  function __construct($a = '', $caller = '') { 
    if(!$caller)
      $caller = new stdClass();
    parent::__construct($a, $caller);
  }
        
  function __init() {/* db_con */
    parent::__init();
  }

  /* runs when a query is posted */
  function query($q, $result_format = '', $src = '', $keep_bnode_ids = 0, $log_query = 0) {
    //$debug = TRUE;

    // if parameter amount is smaller than $max, simply let arc handle it. 
    $max = 5;
    
    // determine the chunksize for queries
    $chunksize = 3;
    
    // give some debug
    if($debug) {
      print_r("Running Query: " . htmlentities($q) . " <br>");  
    }
    
    // Get a SPARQL Plus Parser... perhaps you would like something else here in future
    // esp. if this interface is abstracted, this functionality for query analyzing
    // must be replaced.
    ARC2::inc('SPARQLPlusParser');
    $p = new ARC2_SPARQLPlusParser($this->a, $this);
        
    $p->parse($q, $src);
    $infos = $p->getQueryInfos();

    // look for filters which directly replace variables
    if(preg_match_all('/FILTER.*?\(.*?(\?.*?)=.*?\<(.*?)\>.*?\)(.*?\.|)/', $q, $matches)) {
      
      // what variables are in the filters?
      foreach($matches[1] as $key => $match) {
      
        // trim them and get the replacement for them
        $var = trim($match);
        $replacement = "<" . trim($matches[2][$key]) . ">";

        // if the variable is queried we don't want to replace it because if we
        // replace all variables the query would need rewriting from select to
        // ask.
        $continue = FALSE;
        $res_vars = $infos['query']['result_vars'];
        
        // if there are no result vars, we can savely stop
        if(empty($res_vars))
          break;
        
        foreach($res_vars as $res_var) {
          
          if($res_var['var'] == substr($var, 1)) {
            $continue = TRUE;
            break;
          }
          
        }
        
        // if the variable is in the resulting variables, do nothing
        if($continue)
          continue;
        
        // else first replace the filter
        $q = preg_replace('/FILTER.*?\(.*?(\?.*?)=.*?\<(.*?)\>.*?\)(.*?\.|)/', "", $q);
        // and then the variable for the uri
        $q = str_replace($var, $replacement, $q); 
      }
    }
    
    // less than $max variables? Let ARC do the work!
    if(count($infos['vars']) < $max || $infos['query']['type'] != "select" || (count($infos['query']['result_vars']) == 1 && $infos['query']['result_vars'][0]['aggregate'] == "count" )) {

      $r = parent::query($q, $result_format, $src, $keep_bnode_ids, $log_query);
      // any errors?
      if($err = $this->getErrors()) {
        foreach($err as $error) 
          drupal_set_message("ARC2-ERROR: " . check_plain($error) . " when performing query " . check_plain($q),"error");
      } 

      return $r;
    }

    // If we come here the query is too large for ARC
    //print_r("Query too large!");    
    // extract the triples from the SPARQL-String
    $triples = $this->rebuild_triple_string($infos['query']['pattern']);


    // Calculate the dependencies between the triples and the variables
    $deps = array();
    $deps['vars'] = array();
    $deps['triples'] = array();
    $deps['filter'] = array();

    foreach($infos['vars'] as $var) {
      foreach($triples as $m_key => $triple) {
        if(preg_match("/FILTER.*?\?" . $var ."/", $triple)) {
          $deps['filter'][$var][] = $triple;
        } else if(preg_match("/^\s*\?" . $var . "\s|\s\?" . $var . "\s|\s\?" . $var . '$/', $triple)) {
          $deps['triples'][$triple][] = $var;
          $deps['vars'][$var][] = $triple;
        }
      } 
    }
    
    //drupal_set_message("deps are: " . htmlentities(serialize($deps)));
    
    // $outvalues will contain the value later, initialize it properly.
    $outvalues = array();
    $outvalues['result']['rows'] = array();
//    $i = 0;

    // all vars which are queried
    $vars = $infos['vars'];

    // resort them for performance -> filters first!
    $allvars = $this->sort_vars($vars, $deps);

    //drupal_set_message("all vars: " . serialize($allvars));

    $alloutvalues = array();
    //print_r("doing query " . htmlentities($q));
    // if there are disjoint trees which have to be handled seperately, there
    // are more than one entry in $allvars
    foreach($allvars as $varkey => $vars) {
      
      $i = 0;
      
      // record the time
      $timestart = microtime(TRUE);
      
      // get the value for each variable in the dependency
      while($i < count($vars)) {
        $querystring = "SELECT DISTINCT";
        $querymid = "";      
        $condition = "";

        $curvar = $vars[$i];
        
        // already asked for all the triples of this var?
        if(empty($deps['vars'][$curvar])) {
          $i++;
          continue;
        }

        // build the sparql-string      
        foreach($deps['vars'][$curvar] as $triple) {
          $querymid .= $triple . ' . ';
          foreach($deps['triples'][$triple] as $depvar) {
//            if($depvar != $curvar) {
              if(empty($deps['var_on_var'][$curvar]) || !in_array($depvar, $deps['var_on_var'][$curvar])) {
                $deps['var_on_var'][$curvar][] = $depvar;
                $querystring .= (" ?" . $depvar);
              }

              // don't ask twice for the same triples
            if($depvar != $curvar) {
              if(in_array($triple, $deps['vars'][$depvar])) {
                foreach($deps['vars'][$depvar] as $m_key => $m_triple) {
                  if($m_triple == $triple) {
                    unset($deps['vars'][$depvar][$m_key]);
                    $deps['vars'][$depvar] = array_values($deps['vars'][$depvar]);
                    
                    // if we emptied the triples of another var we should take
                    // and use the constraints of that var
                    //if(empty($deps['vars'][$depvar]) && !empty($deps['filter'][$depvar])) {
                    if(!empty($deps['filter'][$depvar])) {
                      foreach($deps['filter'][$depvar] as $filter) {
                        $condition .= $filter . " . ";
                      }
                    }
                  }
                }
              }
            }
          }
        }
        
        if(isset($deps['filter'][$curvar])) {
          foreach($deps['filter'][$curvar] as $filter) {
            $condition .= $filter . " . ";
          }
        }

        $querystring .= " WHERE { ";
            
        //drupal_set_message("query: " . htmlentities($querystring . $querymid . $condition). "\n");
        // get to work!
        $outvalues = $this->construct_deps(0, $curvar, $deps, $querystring, $querymid, $outvalues, $condition);

        //print_r("outvalues");
        //print_r($outvalues);
        
        // if we did not find anything now we can savely stop it.
        if(empty($outvalues['result']['rows'])) {
//          print_r("Nothing found! Stop! With var $curvar");
          break;
        }
        // next var
        $i++;
      }

      // correct the variables and the time
      $outvalues['result']['variables'] = $vars;
      $outvalues['query_time'] = microtime(TRUE) - $timestart;
      
      $alloutvalues[] = $outvalues;
    }
//    return array();
    // initialize with the first values
    $outvalues = $alloutvalues[0];
    
    // merge if there are more values
    foreach($alloutvalues as $key => $an_outvalue) {
      if($key == 0)
        continue;
      $tmp = $outvalues;
      $outvalues['result']['rows'] = array();
      
      foreach($tmp['result']['rows'] as $m_value) {
        foreach($an_outvalue['result']['rows'] as $an_value) {
          $outvalues['result']['rows'][] = array_merge($m_value, $an_value);
        }
      }
    }

    // what did the user want?
    if($result_format == 'rows')
      $r = isset($outvalues['result']['rows']) ? $outvalues['result']['rows'] : array();
    else if($result_format == "row")
      $r = isset($outvalues['result']['rows'][0]) ? $outvalues['result']['rows'][0] : array();
    else {
      $r = $outvalues;
      if(empty($r)) {
        $r['result']['rows'] = array();
        $r['result']['variables'] = array();
      }
    }
    //drupal_set_message("I return: " . serialize($r));
    //print_r("all out:");
    //print_r($alloutvalues);
    //print_r("<br/>");
    //print_r($r);

    return $r;
  }
  
  
  // sort the variables - filters first!
  function sort_vars($vars, $deps) {
    $sortedvars = array();
    $tmpvars = array();
    $outvars = array();

    // are there any filters?
    foreach($deps['filter'] as $var => $triples) {
      if(!in_array($var, $tmpvars))
        $tmpvars[] = $var;
    }

    // nothing found? then simply take the first one
    if(empty($tmpvars))
      $tmpvars[] = $vars[0];

      
    // construct the dependencies
    while($var = array_shift($tmpvars)) {
      foreach($deps['vars'][$var] as $triple) {
        foreach($deps['triples'][$triple] as $depvar) {
          if($var != $depvar && !in_array($depvar, $tmpvars) && !in_array($depvar,$outvars))
            $tmpvars[] = $depvar;
        }
      }
      if(!in_array($var, $outvars))
        $outvars[] = $var;
    }
    
    // delete the ones which were constructed now
    foreach($vars as $key => $var) {
      if(in_array($var, $outvars))
        unset($vars[$key]);
    }

    // there are no variables left... stop it
    if(empty($vars))
      return array($outvars);
    
    // there are some more in a separate graph!
    $sortedvars = $this->sort_vars($vars, $deps);
    
    $sortedvars[] = $outvars;
    
    return $sortedvars;
  }
  
  
  // now get the triples based on the dependencies
  function construct_deps($i, $curvar, $deps, $querystring, $querymid, $outvalues, $condition) {
    if($i >= count($deps['var_on_var'][$curvar])) {
//      if(!strpos($querystring, '?'))
//        return $outvalues;
//      drupal_set_message("querystring: " . htmlentities($querystring));
      $q = $querystring . $querymid . $condition . " }";
//      print_r("condition: " . htmlentities($condition));
      $out = parent::query($q);
      //print_r("real out: ");
      //print_r($out);
      // any errors?
      if($this->getErrors()) {
        print_r("failed:");
        print_r($q);
      }
      return $out;
    }
    
    // no variables here by now?    
    if(!isset($outvalues['result']['rows'][0][$deps['var_on_var'][$curvar][$i]])) {
      return $this->construct_deps($i+1, $curvar, $deps, $querystring, $querymid, $outvalues, $condition);
    }

    // something found!    
    $all_arr = array();
    $all_arr['result']['rows'] = array();
    $all_arr['result']['variables'] = array();

    $tmpoutvalues = $outvalues['result']['rows'];
    foreach($tmpoutvalues as $vkey => $value ) { // [0][$deps['var_on_var'][$curvar][$i]]['result']['rows'] as $vkey => $value) {
      $querymid = " " . $querymid;
      
      //print_r("<br/>vor der ersetzung: " . htmlentities($querymid));
      if($value[$deps['var_on_var'][$curvar][$i] . " type"] == "literal") {
        $querytmpstring = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' ', $querystring);
        $querytmpmid = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' "' . $value[$deps['var_on_var'][$curvar][$i]] . '" ', $querymid);
        $condition = str_replace("FILTER ( ?" . $deps['var_on_var'][$curvar][$i] . ' = "' . $value[$deps['var_on_var'][$curvar][$i]] . '"  ) .' , "", $condition);
      } else if ($value[$deps['var_on_var'][$curvar][$i] . " type"] == "bnode") {
        $querytmpstring = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' ', $querystring);
        $querytmpmid = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' <' . $value[$deps['var_on_var'][$curvar][$i]] . '> ', $querymid);
        $condition = str_replace("FILTER ( ?" . $deps['var_on_var'][$curvar][$i] . " = <" . $value[$deps['var_on_var'][$curvar][$i]] . ">  ) ." , "", $condition);
      } else {
        $querytmpstring = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' ', $querystring);
        $querytmpmid = str_replace(' ?' . $deps['var_on_var'][$curvar][$i] . ' ', ' <' . $value[$deps['var_on_var'][$curvar][$i]] . '> ', $querymid);
//        print_r("I am looking to replace: " . htmlentities("FILTER ( ?" . $deps['var_on_var'][$curvar][$i] ." = <" . $value[$deps['var_on_var'][$curvar][$i]] . "> ) ."));
//        $condition = str_replace("FILTER ( ?" . $deps['var_on_var'][$curvar][$i] ." = <" . $value[$deps['var_on_var'][$curvar][$i]] . "> ) .", "", $condition);
        $condition = str_replace("FILTER ( ?" . $deps['var_on_var'][$curvar][$i] . " = <" . $value[$deps['var_on_var'][$curvar][$i]] . ">  ) ." , "", $condition);
//        print_r("I did replace: " . htmlentities($condition));
      }
//      print_r("<br/>nach der ersetzung: " . htmlentities($querytmpmid));
      
      if(strpos($querytmpstring, '?') === FALSE) {
        $q = "ASK { " . $querytmpmid . " } " . $condition;
        $out = parent::query($q);
        if(!$out['result'])
          unset($outvalues['result']['rows'][$vkey]);
  
//          drupal_set_message(serialize($out));
        continue;
      }
        
      $arr = $this->construct_deps($i+1, $curvar, $deps, $querytmpstring, $querytmpmid, $tmpoutvalues, $condition);

      $tmparr = array();
    
      if(empty($arr['result']['rows'])) {
        unset($outvalues['result']['rows'][$vkey]);
        continue;
      }
      
      unset($outvalues['result']['rows'][$vkey]);
      foreach($arr['result']['rows'] as $key => $arr_row) {
        $outvalues['result']['rows'][] = array_merge($arr_row, $value);
      }
      

    }
    
    $outvalues['result']['rows'] = array_values($outvalues['result']['rows']);
    
    return $outvalues;
    
  }

  function rebuild_triple_string($array) {
    $res = array();
    if($array['type'] == "group") {
      foreach($array['patterns'] as $arr) 
        $res = array_merge($res, $this->rebuild_triple_string($arr));
      return $res;
    }
    
    if($array['type'] == "triples") {
      foreach($array['patterns'] as $arr)
        $res = array_merge($res, $this->rebuild_triple_string($arr));
      return $res;
    }
    
    if($array['type'] == "triple") {
      $resstr = "";
      if($array['s_type'] == "uri")
        $resstr .= "<" . $array['s'] . ">";
      if($array['s_type'] == "var")
        $resstr .= "?" . $array['s'];
        
      $resstr .= " ";
      
      $resstr .= "<" . $array['p'] . "> ";
      
      if($array['o_type'] == "uri")
        $resstr .= "<" . $array['o'] . ">";
      if($array['o_type'] == "var")
        $resstr .= "?" . $array['o'];
      if($array['o_type'] == "literal")
        $resstr .= '"' . $array['o'] . '"';
      $res[] = $resstr;
      return $res;
    }  
    
    if($array['type'] == "union") {
      $resstr = "{ ";
      $ret = array();
      foreach($array['patterns'] as $arr)
        $ret = array_merge($ret, $this->rebuild_triple_string($arr));
      
      $resstr .= $ret[0];
      
      unset($ret[0]);
      
      foreach($ret as $str)
        $resstr .= " } UNION { " . $str;
      
      $resstr .= " }";
      $res[] = $resstr;
      
      return $res;
    }
    
    if($array['type'] == "filter") {
//      print_r($array);
      $resstr = "FILTER ";
      
      if(isset($array['constraint']['patterns'])) {
        $resstr .= "( ";
        if($array['constraint']['patterns'][0]['type'] == "var")
          $resstr .= "?" . $array['constraint']['patterns'][0]['value'] . " ";
        if($array['constraint']['patterns'][0]['type'] == "uri")
          $resstr .= "<" . $array['constraint']['patterns'][0]['uri'] . "> ";
        if($array['constraint']['patterns'][0]['type'] == "literal")
          $resstr .= '"' . $array['constraint']['patterns'][0]['value'] . '" ';
      
        unset($array['constraint']['patterns'][0]);
      
        foreach($array['constraint']['patterns'] as $pat) {
          if($pat['type'] == "var")
            $resstr .= $array['constraint']['operator'] . " ?" . $pat['value'] . " ";
          if($pat['type'] == "uri")
            $resstr .= $array['constraint']['operator'] . " <" . $pat['uri'] . "> ";
          if($pat['type'] == "literal")
            $resstr .= $array['constraint']['operator'] . ' "' . $pat['value'] . '" ';
        }
      } else if(isset($array['constraint']['call'])) {
        $resstr .= $array['constraint']['call'] . " (";
        if($array['constraint']['args'][0]['type'] == "var")
          $resstr .= "?" . $array['constraint']['args'][0]['value'] . "";
        if($array['constraint']['args'][0]['type'] == "uri")
          $resstr .= "<" . $array['constraint']['args'][0]['uri'] . ">";
        if($array['constraint']['args'][0]['type'] == "literal")
          $resstr .= '"' . $array['constraint']['args'][0]['value'] . '"';
      
        unset($array['constraint']['args'][0]);
      
        foreach($array['constraint']['args'] as $pat) {
          if($pat['type'] == "var")
            $resstr .= ", ?" . $pat['value'] . "";
          if($pat['type'] == "uri")
            $resstr .= ", <" . $pat['uri'] . ">";
          if($pat['type'] == "literal")
            $resstr .= ', "' . $pat['value'] . '"';
        }
      }
      
      
      
      $resstr .= " )";
      $res[] = $resstr;
      
      return $res;
    }
    
    return $res;
  }
                                        
}


class wisski_ARC2Remote extends ARC2_RemoteSPARQLOneDotOneStore {
  protected $update = "";
  protected $query = "";
  protected $construct_parameters = array();
  protected $calling_object = NULL;

  function __construct($a = '', $caller = '') {
    if(!empty($a['remote_update_server']))
      $this->update = $a['remote_update_server'];
    if(!empty($a['remote_query_server']))
      $this->query = $a['remote_query_server'];
    else
      $this->query = $a['remote_store_endpoint'];

    $this->construct_parameters = $a;

    if(!$caller)
      $caller = new stdClass();
      
    $this->calling_object = $caller;
    parent::__construct($a, $caller);
  }
        
  function __init() {/* db_con */
    parent::__init();
  }
  
  function runQuery($q, $qt = '', $infos = '') {
    if(!empty($infos) && ($infos['query']['type'] == "select" || $infos['query']['type'] == "base" ||$infos['query']['type'] == "ask" || $infos['query']['type'] == "construct" || $infos['query']['type'] == "describe") || empty($this->update)) {
      $this->a['remote_store_endpoint'] = $this->query;
      $r = parent::runQuery($q, $qt, $infos);
    } else {
      $this->construct_parameters['remote_store_endpoint'] = $this->update;
      $this->a['remote_store_endpoint'] = $this->update;      
      $r = parent::runQuery($q, $qt, $infos);
    }
    return $r;
  }  
  
  
  function rewriteQuery($q, $t) {
    
    $head = '';
    $body = $q;
        
    if ($t == 'select') {
/*      ARC2::inc('SPARQLPlusParser');
      $p = new ARC2_SPARQLPlusParser($this->a, $this);
      drupal_set_message("q in: " . htmlentities($q) . " time " . microtime());
          
      $p->parse($q, $src);
      $infos = $p->getQueryInfos();
          
      $res_vars = $infos['query']['result_vars'];

      drupal_set_message("after parse: " . htmlentities($q) . " time " . microtime());
*/      
      $tmp = preg_match('/^\s*(?:PREFIX\s+\S+\s+\S+\s+|BASE\s+\S+\s+)*(?:SELECT(?:\s+DISTINCT)?)((?:\s+[\$\?]\S+)+)/sui', $q, $res_vars);
      
      $head = $res_vars[0];
      $body = mb_substr($q, mb_strlen($head));

      $res_vars = preg_split('/\s+/u', $res_vars[1], NULL, PREG_SPLIT_NO_EMPTY);
#dpm(array($q, $tmp, $res_vars, $head, $body,debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS)),'resvars');

    }
    

    // look for filters which directly replace variables
    if(preg_match_all('/FILTER\s*\(\s*([$?]\S*?)\s*=\s*(<[^>]*>).*?\)(\s*\.)?/sui', $body, $matches)) {
      
      // what variables are in the filters?
      foreach($matches[1] as $key => $var) {
        
        // if the variable is queried we don't want to replace it because if we
        // replace all variables the query would need rewriting from select to
        // ask.
        if ($t == 'select' && in_array($var, $res_vars)) {
#dpm(lalala);
          continue;
        }
    

        // get the replacement for them
        $replacement = $matches[2][$key];
        
/*        if ($t == 'select') {
          // if the variable is queried we don't want to replace it because if we
          // replace all variables the query would need rewriting from select to
          // ask.
          $continue = FALSE;
          
          // if there are no result vars, we can savely stop
#        if(empty($res_vars))
#          break;
          
          foreach($res_vars as $res_var) {
            
            if($res_var['var'] == $var) {
              $continue = TRUE;
              break;
            }
            
          }
          
          
          // if the variable is in the resulting variables, do nothing
          if($continue) continue;
        }*/
        
        // else first replace the filter
        $body = str_replace($matches[0][$key], '', $body);
        // and then the variable for the uri
        $body = str_replace($var, $replacement, $body); 
      }
    }    
    
    $q = $head . $body;

#dpm(array($q),'q');

    return $q;

  }


  /* runs when a query is posted */
  function query($q, $result_format = '', $src = '', $keep_bnode_ids = 0, $log_query = 0) {
    // Get a SPARQL Plus Parser... perhaps you would like something else here in future
    // esp. if this interface is abstracted, this functionality for query analyzing
    // must be replaced.

    preg_match('/^(?:\s*(?:PREFIX\s+\S+\s+\S+\s+|BASE\s+\S+\s+)*\s*)(\w+)/sui', $q, $matches);

    $infos['query']['type'] = strtolower($matches[1]);

    $q = $this->rewriteQuery($q, $infos['query']['type']);

    if(in_array($infos['query']['type'], array("select", "ask", "construct", "describe")) || empty($this->update)) {
      $this->a['remote_store_endpoint'] = $this->query;
    } else {
      $this->a['remote_store_endpoint'] = $this->update;
    }
    
    $r = parent::query($q, $result_format, $src, $keep_bnode_ids, $log_query, $infos);

    return $r;
  }
}
