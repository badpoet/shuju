<?php

$last_lat;

class Loc
{
	public $lat;
	public $lng;
	public function __construct($lat1,$lng1)
	{  
        		$this->lat = $lat1;
        		$this->lng = $lng1;

        		$this->lat = $lat1;
        		$this->lng = $lng1;

    	}  

    	public function setloc( $lat1,$lng1 )
    	{
        		$this->lat = $lat1;
        		$this->lng = $lng1;
    	}
}

class Config
{
	public $locnw;
	public $locne;
	public $locsw;
	public $loc_start;
	public $str;
	public $total;

	public function get_lat_from_km( $base,$km )
	{
		$earth_rad = 6378.1;
		$dec = ($km/$earth_rad)*(360/(2*3.141592653));
		return $base - $dec;
	}

	public function get_lng_from_km( $base, $km )
	{
		$earth_rad = 6378.1;
		$inc = $km/$earth_rad*(360/(2*3.141592653));
		return $base + $inc;
	}	

	public function get_lat_from_km_dec( $base,$km )
	{
		$earth_rad = 6378.1;
		$dec = $km/$earth_rad*(360/(2*3.141592653));
		return $base + $dec;
	}

	public function get_lng_from_km_dec( $base, $km )
	{
		$earth_rad = 6378.1;
		$inc = $km/$earth_rad*(360/(2*3.141592653));
		return $base - $inc;
	}

	public function get_pre_one($base)
	{
		$res  = new Loc( $base->lat, $base->lng );
		if( $this->get_lng_from_km_dec( $res->lng,$this->str ) < $this->locnw->lng )
		{
			$res->setloc(  $this->get_lat_from_km_dec( $res->lat,$this->str ), $this->locne->lng );
		}
		else
		{
			$res->setloc(   $res->lat, $this->get_lng_from_km_dec( $res->lng,$this->str ) );
		}
		return $res;
	}

	public function get_next_one( $base )
	{
		$res  = new Loc( $base->lat, $base->lng );
		if( $base->lng >= $this->locne->lng )
		{
			$res->setloc(  $this->get_lat_from_km( $res->lat,$this->str ), $this->locnw->lng );
		}
		else
		{
			$res->setloc(   $res->lat, $this->get_lng_from_km( $res->lng,$this->str ) );
		}
		return $res;
	}

	function get_next_ten( $base )
	{
		$arr=array();
		$tmpbase = $base;
		for ( $i = 0; $i < 10; $i ++)
		{
			$arr[$i] = $this -> get_next_one( $tmpbase );
			$tmpbase = $arr[$i];
		}

		return $arr;
	}

} 


function get_url( $base, $locstr, $other_arg )//form the url which will be send
{
	return $base.$locstr.$other_arg;
}


function loc_glue( array $loc )//sum the all locations
{
	$num = count($loc);
	$res ="";
	for( $i=0;$i<$num; $i++ )
	{
		$res = $res.$loc[$i]->lat.",".$loc[$i]->lng;
		if($i != ($num-1))
			$res = $res."|";
	}
	return $res;
}

function decode( $info, array $request_arr )
{	
	$restmp = json_decode($info,true);
	$res_str = "";
	$error_code = 0;
	
	echo "status: ".$restmp["status"]."\n";
	if( $restmp["status"] == "OK" )
	{
		$num = count($restmp['results']);
		
		for( $i = 0; $i < $num; $i++  )
		{
			if($restmp["results"][$i]["location"]["lat"]!=$GLOBALS['last_lat'])
				$res_str.="next lat\n";
			$res_str.= (string)$restmp["results"][$i]["location"]["lat"]."\t".(string)$restmp["results"][$i]["location"]["lng"]."\t".(string)$restmp["results"][$i]["elevation"]."\t".(string)$restmp["results"][$i]["resolution"]."\n";
			$GLOBALS['last_lat'] = $restmp["results"][$i]["location"]["lat"];
		}
	
	}
	else
	{
		$error_code = 1;
		$num = count($request_arr);
	
		for( $i = 0; $i < $num; $i++  )
		{
			if($request_arr[$i]->lat!=$GLOBALS['last_lat'])
				$res_str.="next lat\n";
			$res_str .= "error_here\t".(string)$request_arr[$i]->lat."\t".(string)$request_arr[$i]->lng."\n";
			$GLOBALS['last_lat'] = $request_arr[$i]->lat;
		}
		echo "can't get data!!!!!!!!!!!!!!!!!!!!!!!!!,please check your net!!!!!!!!!!!!!!!\n";
	}
	echo $res_str;
	$res_arr = array( $res_str, $error_code );
	return $res_arr;
}

$count_file = fopen("count.txt", "a") or die("Unable to open result file!");
$result_file = fopen("result.txt", "a") or die("Unable to open result file!");
$config_file =  fopen("config.txt", "r") or die("Unable to open config file!");


$request_base = "https://maps.googleapis.com/maps/api/elevation/json?locations=";
$other_arg = "&sensor=true_or_false";

 $config = new Config(); 

//get bisic config
 $useless = fscanf($config_file, "%s\n");
$nw = fscanf($config_file, "%lf\t%lf\n");
 list ($tmp1, $tmp2) = $nw;
$config->locnw = new Loc($tmp1,$tmp2);

 $useless = fscanf($config_file, "%s\n");
$ne = fscanf($config_file, "%lf\t%lf\n");
 list ($tmp1, $tmp2) = $ne;
$config->locne = new Loc($tmp1,$tmp2);

 $useless = fscanf($config_file, "%s\n");
$sw = fscanf($config_file, "%lf\t%lf\n");
 list ($tmp1, $tmp2) = $sw;
$config->locsw = new Loc($tmp1,$tmp2);

$useless = fscanf($config_file, "%s\n");
$start = fscanf($config_file, "%lf\t%lf\n");
 list ($tmp1, $tmp2) = $start;
$config->loc_start = new Loc($tmp1,$tmp2);

 $useless = fscanf($config_file, "%s\n");
$now = fscanf($config_file, "%lf\t%d\n");
 list ($tmp1, $tmp2) = $now;
$config->str = $tmp1;
$config->total = $tmp2;

 $useless = fscanf($config_file, "%s\n");
$startpointtmp = fscanf($config_file, "%d\n");
 list ($startpoint) = $startpointtmp;

$current_loc = new Loc( $config->loc_start ->lat, $config->loc_start ->lng  );
$current_loc = $config->get_pre_one($current_loc);

 for( $i = 0; $i<$startpoint; $i++ )
 {
 	$current_loc = $config -> get_next_one( $current_loc );
 }

$last_lat = $current_loc->lat;

//start get data


for( $i = 0; $i < $config->total; $i+=10 )
{
	//todo
	sleep(1);
	
	$arr = $config->get_next_ten( $current_loc );
	$current_loc->setloc($arr[9]->lat,$arr[9]->lng);
	$tmp = file_get_contents(get_url($request_base,loc_glue( $arr ),$other_arg));
	$restmp = decode($tmp,$arr);
	fwrite($result_file, $restmp[0]);
	fwrite($count_file, $i."\n");
	echo   $i."\n";
	if( $restmp[1] != 0 )
	{
		echo $restmp[1];
		fclose($result_file);
		fclose($config_file);
		fclose($count_file);
		exit();
		break;
	}
}

//fwrite($myfile, $tmp);
fclose($count_file);
fclose($result_file);
fclose($config_file);
?>
