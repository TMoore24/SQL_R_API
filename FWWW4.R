###Load in Packages
require("jsonlite") ###Allows R to work with JSON files from API
require("curl")     ### Works with URLs
require("reshape2")  
require("tidyr")
require("stringr")
require("sp")
require("rgdal")
require(RODBC)

#connect to the database
dbConn<- odbcDriverConnect(connection='driver={SQL Server Native Client 11.0}; server=servername,6001;database=databasename;Trusted_Connection=ifnotusernameandpass', believeNRows=FALSE)
#find the max date of info so update get the last date entered for any instument
dt.filt<- sqlQuery(dbConn, "SELECT MIN(Src.myDate) AS minDate
                   FROM
                   (SELECT MAX(Samp_Date) AS myDate
                   FROM tblAirTemperature
                   WHERE SiteID IN('FWWW4')
                   UNION
                   SELECT MAX(Samp_Date) AS myDate
                   FROM tblWind_WindDirection
                   WHERE SiteID IN('FWWW4')
                   UNION
                   SELECT MAX(Samp_Date) AS myDate
                   FROM tblSolarRadiation
                   WHERE SiteID IN('FWWW4')
                   UNION
                   SELECT MAX(Samp_Date) AS myDate
                   FROM tblRelativeHumidity
                   WHERE SiteID IN('FWWW4')
                   UNION
                   SELECT MAX(Samp_Date) AS myDate
                   FROM tblPrecipitation
                   WHERE SiteID IN('FWWW4')) AS Src", as.is = TRUE)
###Pulls from 1st day of data or last date entered so code can renew table and then update
ifelse(is.na(dt.filt$minDate[1]), startDate<- "201509190000", 
       startDate<- format(as.POSIXct(dt.filt$minDate[1], format = "%Y-%m-%d %H:%M:%S"), '%Y%m%d%H%M'))

#####Use system time as the max date? 
time1<-as.Date(Sys.time()) 
###Format time for URL
time<-as.character(format(time1,'%Y%m%d%H%M'))
FWWW4<-paste('http://api.mesowest.net/v2/stations/timeseries?stid=FWWW4&type=all&start=', startDate,'&end=',time,'&token=1234567890', sep="")
sample <- fromJSON(FWWW4) ###read in JSON
###Station info
elevation <- sample$STATION$ELEVATION
station_name <- sample$STATION$NAME
station_id <- sample$STATION$STID
long <- as.numeric(sample$STATION$LONGITUDE)
lat <- as.numeric(sample$STATION$LATITUDE)
####Put station info in table
sitetable <- data.frame(row.names=station_name, station_id, elevation, lat, long)
colnames(sitetable) <- c("ID", "Elevation_ft", "Latitude", "Longitude")
DD<-data.frame(SiteID= station_id, lat=lat,long=long)
DD<- SpatialPointsDataFrame(coords=DD[,c("long", "lat")], data=DD, proj4string = CRS("+proj=longlat +datum=WGS84"))
DB.Coord<-spTransform(DD, CRS("+init=epsg:3857"))
DB.Coord<-as.data.frame(DB.Coord)
DB.Coord$WKT<-paste(DB.Coord$long.1,DB.Coord$lat.1, sep=" ")
Sites<-data.frame(DB.Coord$SiteID,DB.Coord$WKT)
Sites$CRS<-"+init=epsg:3857"
colnames(Sites) <- c("SiteID","WKTShape", "CRS")

###Create dataframes for each table in SQL server
AirTemperature<-data.frame()
Wind_WindDirection<-data.frame()
SolarRadiation<-data.frame()
RelativeHumidity<-data.frame()
Precipitation<-data.frame()
Site<-sample$STATION$STID ####runs all code for each site ***Seperate code for each site now
#####Begin loops putting data from JSON into dataframes
for(i in 1:length(Site)) {
  #############Values, dissectiong the JSON
  Solar_Radiation<- sample$STATION$OBSERVATIONS$solar_radiation_set_1[[i]]
  Wind_Cardinal <- sample$STATION$OBSERVATIONS$wind_cardinal_direction_set_1d[[i]]
  Voltt <- sample$STATION$OBSERVATIONS$volt_set_1[[i]]
  Wind_Gust <- sample$STATION$OBSERVATIONS$ wind_gust_set_1[[i]]
  Dew_Point <- sample$STATION$OBSERVATIONS$dew_point_temperature_set_1d[[i]]
  Wind_Chill <- sample$STATION$OBSERVATIONS$wind_chill_set_1d[[i]]  
  Precip_Accum <- sample$STATION$OBSERVATIONS$precip_accum_set_1[[i]]
  Heat_Index <- sample$STATION$OBSERVATIONS$heat_index_set_1d[[i]]
  Wind_Direction <- sample$STATION$OBSERVATIONS$wind_direction_set_1[[i]]
  Rel_Humid <- sample$STATION$OBSERVATIONS$relative_humidity_set_1[[i]]   
  Wind_Speed <- sample$STATION$OBSERVATIONS$wind_speed_set_1[[i]] 
  Air_Temp<- sample$STATION$OBSERVATIONS$air_temp_set_1[[i]]
  
  
  
  ###################date_times 
  date_time <- sample$STATION$OBSERVATIONS$date_time[[i]]
  FW<-as.data.frame(c(sample$STATION$OBSERVATIONS$date_time[[i]]))
  colnames(FW)<-c("date")
  FW$date<-as.POSIXct(FW$date, format="%Y-%m-%dT %H:%M:%SZ", tz="UTC")
  ###### subtracting 7 hours UTC to MST 
  FW$date_MST<-FW$date-25200
  FW$site<-sample$STATION$STID[[i]]
  
  
  ##AirTemp
  Temp <- data.frame(FW$site,FW$date_MST,FW$date,Air_Temp)
  Temp$isPublished<-"0"
  Temp$Visable<-"1"
  Temp$Observer<-"6"  ####change for user
  Temp$Flag_Indicator<-"NULL"
  Temp$SensorHeight<-"99"
  Temp$InstrumentID<-"NULL"
  Temp<-na.omit(Temp)
  Temp<-Temp[c(1,2,3,9,4,8,7,10,5,6)]
  colnames(Temp) <- c("SiteID","Samp_Date", "Samp_DateGMT","SensorHeight","AirTemperature_C","Flag_Indicator","Observer","InstrumentID","isPublished","Visable")
  Temp<-Temp[(Temp$AirTemperature_C> (-60) & Temp$AirTemperature_C<57),] ####QC http://mesowest.utah.edu/cgi-bin/droman/variable_select.cgi?order=long
  
  ##Wind
  Wind <- data.frame(FW$site,FW$date_MST,FW$date,Wind_Speed,Wind_Direction, Wind_Gust)
  Wind$StandardDeviationWindDirection<-"NULL"
  Wind$isPublished<-"0"
  Wind$Visable<-"1"
  Wind$Observer<-"6"  ####change for user
  Wind$Flag_Indicator<-"NULL"
  Wind$SensorHeight<-"99"
  Wind$InstrumentID<-"NULL"
  Wind<-Wind[c(1,2,3,11,4,5,6,7,11,10,13,8,9)]
  colnames(Wind) <- c("SiteID","Samp_Date", "Samp_DateGMT","SensorHeight","WindSpeed_m_s","WindDirection_DegPastNorth","WindGust_m_s","StandardDeviationWindDirection","Flag_Indicator","Observer","InstrumentID","isPublished","Visable")
  Wind<-Wind[!is.na(Wind$WindSpeed_m_s & is.na(Wind$WindSpeed_m_s)),]
  Wind<-Wind[(Wind$WindSpeed_m_s>= 0 & Wind$WindSpeed_m_s<103),]           ####QC
  #Wind[is.na(Wind)]<-"NULL"
  Wind<-Wind[( Wind$WindDirection_DegPastNorth>= 0 & Wind$WindDirection_DegPastNorth<=360),]
  Wind<-Wind[(Wind$WindGust_m_s>= 0 & Wind$WindGust_m_s<129),]
  Wind<-na.omit(Wind)
  #####Radiation
  Sun <- data.frame(FW$site,FW$date_MST,FW$date,Solar_Radiation)
  Sun$Shortwave_Out_W_m2<-"NULL"
  Sun$Longwave_In_W_m2<-"NULL"
  Sun$Longwave_Out_W_m2<-"NULL"
  Sun$NetRadiation_W_m2<-"NULL"
  Sun$TempCorrectLongwave_Out_W_m2<-"NULL"
  Sun$TempCorrectLongwave_In_W_m2<-"NULL"
  Sun$TotalRadiation_Out_W_m2<-"NULL"
  Sun$Shortwave_In_FluxDensity_kW_m2<-"NULL"
  Sun$NetLongwaveRadiation_W_m2<-"NULL"
  Sun$NetShortwaveRadiation_W_m2<-"NULL"
  Sun$Flag_Indicator<-"NULL"
  Sun$Observer<-"6"   ####change for user
  Sun$InstrumentID<-"NULL"
  Sun$isPublished<-"0"
  Sun$Visable<-"1"
  Sun<-na.omit(Sun)
  colnames(Sun) <- c("SiteID","Samp_Date", "Samp_DateGMT","Shortwave_In_W_m2","Shortwave_Out_W_m2","Longwave_In_W_m2","Longwave_Out_W_m2","NetRadiation_W_m2","TempCorrectLongwave_Out_W_m2","TempCorrectLongwave_In_W_m2","TotalRadiation_Out_W_m2","Shortwave_In_FluxDensity_kW_m2","NetLongwaveRadiation_W_m2","NetShortwaveRadiation_W_m2","FlagIndicator","Observer","InstrumentID","isPublished","Visable")
  Sun<-Sun[(Sun$Shortwave_In_W_m2> (-500) & Sun$Shortwave_In_W_m2<1000),] ##QC
  
  ###Humidity   Time averaged Humidity column not in database right now
  Humid <- data.frame(FW$site,FW$date_MST,FW$date,Rel_Humid)
  Humid$RelativeHumidityMaxPct<-"NULL"
  Humid$RelativeHumidityMinPct<-"NULL"
  Humid$isPublished<-"0"
  Humid$Visable<-"1"
  Humid$Observer<-"6"   ####change for user
  Humid$Flag_Indicator<-"NULL"
  Humid$SensorHeight<-"99"
  Humid$InstrumentID<-"NULL"
  Humid<-na.omit(Humid)
  Humid<-Humid[c(1,2,3,11,4,5,6,10,9,11,7,8)]
  colnames(Humid) <- c("SiteID","Samp_Date", "Samp_DateGMT","SensorHeight","RelativeHumidityTimeAVG","RelativeHumidityMaxPct","RelativeHumidityMinPct","Flag_Indicator","Observer","InstrumentID","isPublished","Visable")
  Humid<-Humid[(Humid$RelativeHumidityTimeAVG> 0 & Humid$RelativeHumidityTimeAVG<=100),] ###QC
  
  ########Precip  accumulation starts on high value, to high for single storm event? also get rid of NAs
  ###taking accumulation and creating precipitation events.
  ###Precipitation table
  Accum<-data.frame(FW$site,Precip_Accum,FW$date_MST,FW$date)
  Accum<-na.omit(Accum)
  rain<- data.frame(Accum$FW.site,Accum$FW.date_MST,Accum$FW.date)
  rain$Rain_Total_mm<-ave(Accum$Precip_Accum,FUN=function(x) c(0,diff(x)))
  rain$isPublished<-"0"
  rain$Visable<-"1"
  rain$Observer<-"6"   ####change for user
  rain$Flag_Indicator<-"NULL"
  rain$InstrumentID<-"NULL"
  rain<-rain[c(1,2,3,4,8,7,9,5,6)]
  colnames(rain) <- c("SiteID","Samp_Date", "Samp_DateGMT","Rain_Total_mm","Flag_Indicator","Observer","InstrumentID","isPublished","Visable")
  rain<-rain[(rain$Rain_Total_mm>= 0 & rain$Rain_Total_mm<76),] ##QC   do we want 0mm rain events? if not remove = from >=
  
  ######### Output tables all sites combined, "bad" data removed via Mesowest standards
  AirTemperature<-rbind(AirTemperature, Temp)
  Wind_WindDirection<-rbind(Wind_WindDirection, Wind)
  SolarRadiation<-rbind(SolarRadiation, Sun)              ###Stations collect at different time intervals.
  RelativeHumidity<-rbind(RelativeHumidity, Humid)
  Precipitation<-rbind(Precipitation, rain)
}


#############Change server, uid, and password which are all ******
#####Update SQL tables from the dataframes above ***May need to change names to reflect server
dbConn1<- odbcDriverConnect(connection='driver={SQL Server Native Client 11.0}; server=,6001;database=;Trusted_Connection=Yes', believeNRows=FALSE)
for(i in seq_len(nrow(Sites))){
  
  query<- paste0(
    "Insert INTO tblSites (SiteID, WKTShape, CRS)
    Values (''",Sites$SiteID[i],"'',
    ''",Sites$WKTShape[i],"'',
    ''",Sites$CRS[i],"'')"
    
  )
  sqlQuery(dbConn1, query)
}


for(i in seq_len(nrow(AirTemperature))){
  
  query<- paste0(
    "Insert INTO tblAirTemperature (SiteID, Samp_Date, Samp_DateGMT, SensorHeight, AirTemperature_C, Flag_Indicator, ObserverID, InstrumentID, isPublished, Visable)
    Values (''",AirTemperature$SiteID[i],"'',
    ''",AirTemperature$Samp_Date[i],"'',
    ''",AirTemperature$Samp_DateGMT[i],"'',
    ",AirTemperature$SensorHeight[i],",
    ",AirTemperature$AirTemperature_C[i],",
    ",AirTemperature$Flag_Indicator[i],",
    ''",AirTemperature$Observer[i],"'',
    ",AirTemperature$InstrumentID[i],",
    ",AirTemperature$isPublished[i],",
    ",AirTemperature$Visable[i],")"
    
  )
  sqlQuery(dbConn1, query)
}


for(i in seq_len(nrow(Precipitation))){
  
  query<- paste0(
    "Insert INTO tblPrecipitation (SiteID, Samp_Date, Samp_DateGMT, Rain_Total_mm, Flag_Indicator, ObserverID, InstrumentID, isPublished, Visable)
    Values (''",Precipitation$SiteID[i],"'',
    ''",Precipitation$Samp_Date[i],"'',
    ''",Precipitation$Samp_DateGMT[i],"'',
    ",Precipitation$Rain_Total_mm[i],",
    ",Precipitation$Flag_Indicator[i],",
    ''",Precipitation$Observer[i],"'',
    ",Precipitation$InstrumentID[i],",
    ",Precipitation$isPublished[i],",
    ",Precipitation$Visable[i],")"
    
  )
  sqlQuery(dbConn1, query)
}

for(i in seq_len(nrow(RelativeHumidity))){
  
  query<- paste0(
    "Insert INTO tblRelativeHumidity (SiteID, Samp_Date, Samp_DateGMT, SensorHeight,RelativeHumidityAVGPct, RelativeHumidityMaxPct,RelativeHumidityMinPct, Flag_Indicator, ObserverID, InstrumentID, isPublished, Visable)
    Values (''",RelativeHumidity$SiteID[i],"'',
    ''",RelativeHumidity$Samp_Date[i],"'',
    ''",RelativeHumidity$Samp_DateGMT[i],"'',
    ",RelativeHumidity$SensorHeight[i],",
    ",RelativeHumidity$RelativeHumidityTimeAVG[i],",
    ",RelativeHumidity$RelativeHumidityMaxPct[i],",
    ",RelativeHumidity$RelativeHumidityMinPct[i],",
    ",RelativeHumidity$Flag_Indicator[i],",
    ''",RelativeHumidity$Observer[i],"'',
    ",RelativeHumidity$InstrumentID[i],",
    ",RelativeHumidity$isPublished[i],",
    ",RelativeHumidity$Visable[i],")"
    
  )
  sqlQuery(dbConn1, query)
}

for(i in seq_len(nrow(SolarRadiation))){
  
  query<- paste0(
    "Insert INTO tblSolarRadiation (SiteID, Samp_Date, Samp_DateGMT, Shortwave_In_W_m2,Shortwave_Out_W_m2, Longwave_In_W_m2,Longwave_Out_W_m2,NetRadiation_W_m2,TempCorrectLongwave_Out_W_m2,TempCorrectLongwave_In_W_m2, TotalRadiation_Out_W_m2,Shortwave_In_FluxDensity_kW_m2,NetLongwaveRadiation_W_m2,NetShortwaveRadiation_W_m2,Flag_Indicator, ObserverID, InstrumentID, isPublished, Visable)
    Values (''",SolarRadiation$SiteID[i],"'',
    ''",SolarRadiation$Samp_Date[i],"'',
    ''",SolarRadiation$Samp_DateGMT[i],"'',
    ",SolarRadiation$Shortwave_In_W_m2[i],",
    ",SolarRadiation$Shortwave_Out_W_m2[i],",
    ",SolarRadiation$Longwave_In_W_m2[i],",
    ",SolarRadiation$Longwave_Out_W_m2[i],",
    ",SolarRadiation$NetRadiation_W_m2[i],",
    ",SolarRadiation$TempCorrectLongwave_Out_W_m2[i],",
    ",SolarRadiation$TempCorrectLongwave_In_W_m2[i],",
    ",SolarRadiation$TotalRadiation_Out_W_m2[i],",
    ",SolarRadiation$Shortwave_In_FluxDensity_kW_m2[i],",
    ",SolarRadiation$NetLongwaveRadiation_W_m2[i],",
    ",SolarRadiation$NetShortwaveRadiation_W_m2[i],",
    ",SolarRadiation$FlagIndicator[i],",
    ''",SolarRadiation$Observer[i],"'',
    ",SolarRadiation$InstrumentID[i],",
    ",SolarRadiation$isPublished[i],",
    ",SolarRadiation$Visable[i],")"
    
  )
  l<- sqlQuery(dbConn1, query)
}

for(i in seq_len(nrow(Wind_WindDirection))){
  
  query<- paste0(
    "Insert INTO tblWind_WindDirection (SiteID, Samp_Date, Samp_DateGMT, SensorHeight,WindSpeed_m_s, WindDirection_DegPastNorth,StandardDeviationWindDirection,WindGusts_m_s, Flag_Indicator, ObserverID, InstrumentID, isPublished, Visable)
    Values (''",Wind_WindDirection$SiteID[i],"'',
    ''",Wind_WindDirection$Samp_Date[i],"'',
    ''",Wind_WindDirection$Samp_DateGMT[i],"'',
    ",Wind_WindDirection$SensorHeight[i],",
    ",Wind_WindDirection$WindSpeed_m_s[i],",
    ",Wind_WindDirection$WindDirection_DegPastNorth[i],",
    ",Wind_WindDirection$StandardDeviationWindDirection[i],",
    ",Wind_WindDirection$WindGust_m_s[i],",
    ",Wind_WindDirection$Flag_Indicator[i],",
    ''",Wind_WindDirection$Observer[i],"'',
    ",Wind_WindDirection$InstrumentID[i],",
    ",Wind_WindDirection$isPublished[i],",
    ",Wind_WindDirection$Visable[i],")"
    
  )
  sqlQuery(dbConn1, query)
}
