# Revision de siniestros de las estaciones del parametrico bajo la informacion del historico del parametrico
rm(list=ls())
library(xlsx)

# lectura de archivos de Triggers del Portafolio modificado 2016, parametros del presente año
rut1<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/"
Triggers<-read.table(paste(rut1,"Triggers_Portafolio_2016_Final.csv",sep=""),header = T,sep=",",fileEncoding = "UTF-8")
Triggers<-Triggers[!is.na(Triggers[,1]),]
Triggers<-Triggers[,c(1:28)] # Quitar columnas extras
# Modificar fechas del archivo Triggers
Ff<-c("Inicio","Final","Inicio.1","Final.1","Inicio.2","Final.2")
for (o in 1:length(Ff)){
  Triggers[,Ff[o]]<-as.Date(as.character(Triggers[,Ff[o]]),format="%d/%m/%Y")  
}
# Cambiar el nombre desde aqui
names(Triggers)<-c("NO.","STATE","ID.GASIR","CLAVE.SMN","ID.PARAM","NOMBRE_ESTACION","LONDEC","LATDEC","LONGITUD","LATITUD","SURFACE.HA","CROP","RIESGO","T.E1","DateFromP1","DateToP1","T.E2.1","T.E2.2","DateFromP2","DateToP2","T.E3.1","T.E3.2","DateFromP3","DateToP3","CUOTA","COSTO_HA","SUMA_ASEGURADA","PRIMA")
# Conexión a la base de datos y consulta de información desde la página del parametrico
library( RMySQL )
# Consultar Tabla  de La Mezcla y Comparar todos los datos
Consulta<-paste("SELECT * FROM dlluvia WHERE FECHA >='2016-01-01'",sep="")
# Se Crea conexion con la base
conexion <- dbConnect( dbDriver("MySQL"), user="agroclimac445786",password="cumulus62",dbname="regiones_agroclimac445786", host="sql5c75a.carrierzone.com")
Test<-dbGetQuery(conexion ,Consulta)
dbDisconnect(conexion)

# Agrupar en una lista los datos de las estaciones recuperados
FechaMax<-max(Test$Fecha)  # Fecha Máxima que contienen los datos
Test<-Test[order(Test$Fecha,Test$idestacion),] # Ordenamiento 
DatosPrec<-split(Test[,c("Fecha","idestacion","Lluvia")],Test$idestacion) # Poner en forma de lista los datos

# Datos de Prueba
TRIGGERS<-Triggers[170,]
DatosPPP<-DatosPrec

##############################################################################
# Generacion de funcion sobre los datos a procesar en listado
# Primer paso busqueda de la informacion a cargar
Calc_Siniestro_Simula<-function(TRIGGERS, DatosPPP){
  # Definir dataframe 
  TRIGGERS<-data.frame((TRIGGERS))
  Est<-TRIGGERS[,"ID.PARAM"]
  # Nombres de estaciones
  names(TRIGGERS)<-c("NO.","STATE","ID.GASIR","CLAVE.SMN","ID.PARAM","NOMBRE_ESTACION","LONDEC","LATDEC","LONGITUD","LATITUD","SURFACE.HA","CROP","RIESGO","T.E1","DateFromP1","DateToP1","T.E2.1","T.E2.2","DateFromP2","DateToP2","T.E3.1","T.E3.2","DateFromP3","DateToP3","CUOTA","COSTO_HA","SUMA_ASEGURADA","PRIMA")
  print(paste("Comienzo No.: ",TRIGGERS[,"NO."]," Estado: ",TRIGGERS[,"STATE"]," Estación: ",Est," Cultivo: ",TRIGGERS$CROP,sep=""))
  # Tiene que comparar los datos para revisar la fecha para saber si es procesada o no en la Etapa correspondiente
  f.Est<-grep(Est,names(DatosPPP),fixed = T) # Busqueda de archivo de la simulacion ENSO
  DatosBase<-DatosPPP[[f.Est]]
  DatosBase$Fecha<-as.Date(DatosBase$Fecha)
  # Comparar si ya inicio alguna de sus etapas
  Maxf<-max(DatosBase$Fecha)
  Vec2<-c("DateFromP1","DateFromP2","DateFromP3")
  FII<-logical(); for (ii in 1:3){TTT<-as.Date(as.character(TRIGGERS[,Vec2[ii]])); if (is.na(TTT)){FII[Vec2[ii]]<-NA} else  FII[Vec2[ii]] <- Maxf>TTT}
  ################################################################
  # Se cargan las funciones utilizadas para el Seguimiento del estado del parametrico
  source("~/Documentos/Documentos/Scripts/Seguimiento_Parametrico/Funciones_Seguimiento_Parametrico_2016.R")
  # Si ya empezó el periodo de la estacion se procede a calcular
  if (sum(FII,na.rm = T) > 0){
    ############################################
    # Se busca en la carpeta los acumulados a procesar
    ruta_acum<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/ACUMULADOS_PREC_PORTAFOLIO/"
    nom_acum<-paste(ruta_acum,"Acum_",TRIGGERS$STATE,"_",(TRIGGERS$CROP),"_",TRIGGERS$ID.PARAM,".csv",sep="")
    if (!file.exists(ruta_acum)){dir.create(ruta_acum)}
    if (file.exists(nom_acum)){
      Acum_Etapa2<-read.table(nom_acum,header = T,sep=",",fileEncoding="latin1",na.strings = c(NA,"ND"))
      Acum_Etapa2<-Acum_Etapa2[!is.na(Acum_Etapa2$ANIO),]
      Acum_Etapa<-split(Acum_Etapa2,Acum_Etapa2$ANIO)
    }
    if (!file.exists(nom_acum)){
      # Acomodo de datos y relleno de no datos
      Per<-as.Date(c(min(DatosBase$Fecha),max(DatosBase$Fecha)))
      Periodo<-data.frame(Fecha=seq(Per[1],Per[2],by="day"))
      DatosBase.1<-merge(Periodo,DatosBase,by="Fecha",all.x = T)
      Datos<-DatosBase.1[,c(1,3)]
      # Lectura de archivo de simulacion de enso
      names(Datos)<-c("FECHA","PREC")
      # Carga de funcion que hace los acumulados
      Cult.Ini.1<-TRIGGERS$DateFromP1;Cult.Ini.2<-TRIGGERS$DateFromP2; Cult.Ini.3<-TRIGGERS$DateFromP3
      Cult.Fin.1<-TRIGGERS$DateToP1; Cult.Fin.2<-TRIGGERS$DateToP2; Cult.Fin.3<-TRIGGERS$DateToP3;
      DDatos<-Datos
      ################################################################
      #  Calculos de los acumulados por etapa estacion y cultivo
      Datos.Anio<-split(Datos,format.Date(Datos$FECHA,format="%Y"))
      Acum_Etapa<- lapply(Datos.Anio, function(x) Ext_Etapa(x, Cult.Ini.1,Cult.Fin.1,Cult.Ini.2,Cult.Fin.2,Cult.Ini.3,Cult.Fin.3))
      Acum_Etapa2<-do.call("rbind",Acum_Etapa)
      # Proceso de escritura de archivo en caso de no existir
      write.table(Acum_Etapa2,file = nom_acum,append = F,quote = F,sep = ",",na="ND",row.names = F,col.names = T, fileEncoding="latin1")
      print(paste("Calculo de Acumulados de Precipitación estación: ",Est,sep=""))
    } # Cierre de proceso en caso de no haber acumulados calculados
    ################################################################
    # Contar los días consecutivos en la etapa 2 para cada estacion si es que ya inicio
    if (FII[2]==TRUE){
      # Se busca en la carpeta los DCSLL a procesar
      ruta_acum_<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/CONTEO_DCSLL"
      nom_acum_<-paste(ruta_acum_,"/DCSP_",TRIGGERS$STATE,"_",(TRIGGERS$CROP),"_",TRIGGERS$ID.PARAM,"_PARAMETRICO_2016.csv",sep="")
      if (!file.exists(ruta_acum_)){dir.create(ruta_acum_)}
      if (file.exists(nom_acum_)){
        DCSP2<-read.table(nom_acum_,header = T,sep=",",fileEncoding="latin1",na.strings = c(NA,"ND"))
        DCSP2<-DCSP2[!is.na(DCSP2$DCSLluvia),]
      }
      if (!file.exists(nom_acum_)){
        ###################################################################
        #  Extraccion de los datos por etapa estacion y cultivo
        # Acomodo de datos y relleno de no datos
        Per<-as.Date(c(min(DatosBase$Fecha),max(DatosBase$Fecha)))
        Periodo<-data.frame(Fecha=seq(Per[1],Per[2],by="day"))
        DatosBase.1<-merge(Periodo,DatosBase,by="Fecha",all.x = T)
        Datos<-DatosBase.1[,c(1,3)]; names(Datos)<-c("FECHA","PREC")
        Datos.Anio<-split(Datos,format.Date(Datos$FECHA,format="%Y"))
        # Se prueban los datos a extraer para la etapa 2 a evaluar
        Datos_Etapa2<-lapply(Datos.Anio, function(x) Ext_Etapa2(x, Cult.Ini.2,Cult.Fin.2))
        # Conteo de días consecutivos sin lluvia por año
        DCSP<-lapply(Datos_Etapa2, function(x) P_DCSLluvia(x))
        DCSP2<-do.call("rbind",DCSP)
        # Proceso de escritura de archivo en caso de no existir
        write.table(DCSP2,file = nom_acum_,append = F,quote = F,sep = ",",na="ND",row.names = F,col.names = T, fileEncoding="latin1")
        print(paste("Calculo de Días consecutivos sin lluvia estación: ",Est,sep=""))
      } # Cierre if de escritura de la estacion
    } # Finaliza conteo de días consecutivos sin lluvia para la estación  corresponiente
    ################################################################
    # Si ya empezó el periodo de la estacion se procede a calcular
    # Nombre del cultivo a revisar
    Cultivo<-as.character(TRIGGERS$CROP)
    # Variables de  apoyo o auxiliares
    Cult.Ini.1<-TRIGGERS$DateFromP1;Cult.Ini.2<-TRIGGERS$DateFromP2; Cult.Ini.3<-TRIGGERS$DateFromP3
    Cult.Fin.1<-TRIGGERS$DateToP1; Cult.Fin.2<-TRIGGERS$DateToP2; Cult.Fin.3<-TRIGGERS$DateToP3;
    # COnteo de dias restantes
    DRE1<-as.numeric(Cult.Fin.1 - Cult.Ini.1)+1
    DRE2<-as.numeric(Cult.Fin.2 - Cult.Ini.2)+1
    DRE3<-as.numeric(Cult.Fin.3 - Cult.Ini.3)+1
    if(is.na(FII[1])){ DRE1<-NA }
    if(!is.na(FII[1]) &  FII[1]==TRUE){ DRE1<-as.numeric(Cult.Fin.1 - Maxf)}
    if(!is.na(FII[2]) &  FII[2]==TRUE){ DRE2<-as.numeric(Cult.Fin.2 - Maxf) }
    if(!is.na(FII[3]) &  FII[3]==TRUE){ DRE3<-as.numeric(Cult.Fin.3 - Maxf) }
    
    # Calculos de los conteos de la información
    # Ciclo final de conteos de informacion
    if (toupper(TRIGGERS$STATE)=="TLAXCALA"){
      Vec.Trigg<-c("T.E1","T.E2.1","T.E2.2","T.E3.1","T.E3.2")
      TRI<-TRIGGERS[,Vec.Trigg]
      Trig.2<-TRIGGERS
      # Acomodo de triggers
      for (jk in 1:ncol(TRI)){
        nom<-names(TRI)[jk];
        Ttt<-as.numeric(unlist(strsplit(as.character(TRI[1,jk]),split="/")))
        DTRI<-data.frame(nom,t(Ttt));colnames(DTRI)<-c("Trigger","A","B")
        if (jk == 1){TRI2<-DTRI}
        else{TRI2<-rbind(TRI2,DTRI)}
      }
      # Variables auxliares para evitar perdida de informacion
      ru.1<-NA;ru.2a<-NA;ru.2b<-NA;ru.3a<-NA;ru.3b<-NA;
      dif.1<-as.numeric(as.character(TRI2[1,"B"]))-as.numeric(as.character(TRI2[1,"A"]))
      dif.2a<-as.numeric(as.character(TRI2[2,"B"]))-as.numeric(as.character(TRI2[2,"A"]))
      dif.2b<-as.numeric(as.character(TRI2[3,"B"]))-as.numeric(as.character(TRI2[3,"A"]))
      dif.3a<-as.numeric(as.character(TRI2[4,"B"]))-as.numeric(as.character(TRI2[4,"A"]))
      dif.3b<-as.numeric(as.character(TRI2[5,"B"]))-as.numeric(as.character(TRI2[5,"A"]))
      # Calculo de los pagos parciales
      for (j in 1: length(Acum_Etapa)){
        aniop<-Acum_Etapa[[j]][1,1]
        nn<-is.na(Acum_Etapa[[j]][,2]); # Comprobar que exista dato de acumulados de lluvia
        # Banderas para el caso de que no haya datos en la "Etapa 1"
        if (nn=="TRUE"){Trig.2[,"DEF.E1"] <-NA;
        Trig.2[,"DIFACUM.DEF.E1"]<-NA}
        ##########################################################
        # Revision de siniestro total
        # Etapa 1
        # Error por falta de lluvia
        Log.DEF.E1<-Acum_Etapa[[j]][,2] < as.numeric(as.character(TRI2[1,"A"]))
        if (!is.na(Log.DEF.E1)){
          if (nn=="FALSE"){
            if (Log.DEF.E1==TRUE){ # Esto quiere decir que se siniestra pago binario
              Trig.2[,"DEF.E1"] <-sum(Log.DEF.E1,na.rm = T)
              }
            if (Log.DEF.E1==FALSE){ # revision de posible pago parcial
              Log.DEF.E1PP<-Acum_Etapa[[j]][,2] < as.numeric(as.character(TRI2[1,"B"]))  
              if (Log.DEF.E1PP==TRUE){
                ru.1<-as.numeric(as.character(TRI2[1,"B"])) - Acum_Etapa[[j]][,2]
                Mont<-(1500/dif.1) * ru.1
                Trig.2[,"DEF.E1"] <-Mont; 
              }
              if (Log.DEF.E1PP==FALSE){Trig.2[,"DEF.E1"] <-0; }
            } # If posible pago parcial
          } # If de si hay dato de lluvia
        } # If prueba de falta de lluvia
        if (is.na(Log.DEF.E1)){Trig.2[,c("DEF.E1")]<-0}
        #####################################
        # Etapa 2
        # Deficit
        Log.DEF.E2<-Acum_Etapa[[j]][,4] < as.numeric(as.character(TRI2[2,"A"]))
        if (!is.na(Log.DEF.E2)){
          if (Log.DEF.E2==TRUE){ # Esto quiere decir que se siniestra pago binario
            Trig.2[,"DEF.E2"] <-sum(Log.DEF.E2,na.rm = T)
          }
          if (Log.DEF.E2==FALSE){ # revision de posible pago parcial
            Log.DEF.E2A<-Acum_Etapa[[j]][,4] < as.numeric(as.character(TRI2[2,"B"]))  
            if (Log.DEF.E2A==TRUE){
              ru.2a<-as.numeric(as.character(TRI2[2,"B"])) - Acum_Etapa[[j]][,4]
              Mont<-(1500/dif.2a) * ru.2a
              Trig.2[,"DEF.E2"] <-Mont;         }
            if (Log.DEF.E2A==FALSE){Trig.2[,"DEF.E2"] <-0;}
          } # If posible pago parcial
        } # If prueba de falta de lluvia
        if (is.na(Log.DEF.E2)){Trig.2[,c("DEF.E2")]<-c(0)}
        # Exceso
        Log.EXC.E2<-Acum_Etapa[[j]][,4] > as.numeric(as.character(TRI2[3,"B"]))
        if (!is.na(Log.EXC.E2)){
          if (Log.EXC.E2==TRUE){ # Esto quiere decir que se siniestra pago binario
            Trig.2[,"EXC.E2"] <-sum(Log.EXC.E2,na.rm = T)
            
          }
          if (Log.EXC.E2==FALSE){ # revision de posible pago parcial
            Log.EXC.E2A<-Acum_Etapa[[j]][,4] > as.numeric(as.character(TRI2[3,"A"]))  
            if (Log.EXC.E2A==TRUE){
              ru.2b<-abs(as.numeric(as.character(TRI2[3,"A"])) - Acum_Etapa[[j]][,4])
              Mont<-(1500/dif.2b) * ru.2b
              Trig.2[,"EXC.E2"] <-Mont; 
            }
            if (Log.EXC.E2A==FALSE){Trig.2[,"EXC.E2"] <-0; }
          } # If posible pago parcial
        } # If prueba de falta de lluvia
        if (is.na(Log.EXC.E2)){Trig.2[,c("EXC.E2")]<-c(0)}
        #####################################
        # Etapa 3
        # Deficit
        Log.DEF.E3<-Acum_Etapa[[j]][,6] < as.numeric(as.character(TRI2[4,"A"]))
        if (!is.na(Log.DEF.E3)){
          if (Log.DEF.E3==TRUE){ # Esto quiere decir que se siniestra pago binario
            Trig.2[,"DEF.E3"] <-sum(Log.DEF.E3,na.rm = T)
          }
          if (Log.DEF.E3==FALSE){ # revision de posible pago parcial
            Log.DEF.E3A<-Acum_Etapa[[j]][,6] < as.numeric(as.character(TRI2[4,"B"]))  
            if (Log.DEF.E3A==TRUE){
              ru.3a<-as.numeric(as.character(TRI2[4,"B"])) - Acum_Etapa[[j]][,6]
              Mont<-(1500/dif.3a) * ru.3a
              Trig.2[,"DEF.E3"] <-Mont; 
            }
            if (Log.DEF.E3A==FALSE){Trig.2[,"DEF.E3"] <-0;}
          } # If posible pago parcial
        } 
        if (is.na(Log.DEF.E3)){Trig.2[,c("DEF.E3")]<-c(0)}
        # Exceso
        Log.EXC.E3<-Acum_Etapa[[j]][,6] > as.numeric(as.character(TRI2[5,"B"]))
        if (!is.na(Log.EXC.E3)){
          if (Log.EXC.E3==TRUE){ # Esto quiere decir que se siniestra pago binario
            Trig.2[,"EXC.E3"] <-sum(Log.EXC.E3,na.rm = T)
          }
          if (Log.EXC.E3==FALSE){ # revision de posible pago parcial
            Log.EXC.E3A<-Acum_Etapa[[j]][,6] > as.numeric(as.character(TRI2[5,"A"]))  
            if (Log.EXC.E3A==TRUE){
              ru.3b<-abs(as.numeric(as.character(TRI2[5,"A"])) - Acum_Etapa[[j]][,6])
              Mont<-(1500/dif.3b) * ru.3b
              Trig.2[,"EXC.E3"] <-Mont;
            }
            if (Log.EXC.E3A==FALSE){Trig.2[,"EXC.E3"] <-0;}
          } # If posible pago parcial
        }
        if (is.na(Log.EXC.E3)){Trig.2[,c("EXC.E3")]<-c(0)}
        ###############################################
        # Poner na's a la mayoria de los valores
        Trig.2[,c("TOTAL.SIN","DIFACUM.DEF.E1","DIFACUM.DEF.E2","DIFACUM.EXC.E2","DIFACUM.DEF.E3","DIFACUM.EXC.E3","ACUM.E1","ACUM.E2","ACUM.E3","POR.DAT.E1","POR.DAT.E2","POR.DAT.E3","DIAS.REST.E1","DIAS.REST.E2","DIAS.REST.E3","DCSLluvia","DCSPFInicial","DCSPFFinal","COSTO_TOTAL")]<-NA
        ###############################################
        # Calcular el monto total a pagar 
        vec2<-c("DEF.E1","DEF.E2","EXC.E2","DEF.E3","EXC.E3")
        sum<-0; band<-0
        for (kj in 1:length(vec2)){
          if (Trig.2[1,vec2[kj]]>1){sum<-sum + Trig.2[1,vec2[kj]]}
          if (Trig.2[1,vec2[kj]]==1){band<-1}
        }
        # Total.Siniestros
        Trig.2[,"TOTAL.SIN"]<-sum(Trig.2[,"EXC.E3"]>0,Trig.2[,"DEF.E3"]>0,Trig.2[,"EXC.E2"]>0,Trig.2[,"DEF.E2"]>0,Trig.2[,"DEF.E1"]>0,na.rm = T)
        # Datos de precipitacion acumulada
        Trig.2[,c("ACUM.E1")]<-c(Acum_Etapa[[j]][,2])
        Trig.2[,c("ACUM.E2")]<-c(Acum_Etapa[[j]][,4])
        Trig.2[,c("ACUM.E3")]<-c(Acum_Etapa[[j]][,6])
        # Escribe el porcentaje de datos en cada etapa
        Trig.2[,"POR.DAT.E1"]<-Acum_Etapa[[j]][,3]
        Trig.2[,"POR.DAT.E2"]<-Acum_Etapa[[j]][,5]
        Trig.2[,"POR.DAT.E3"]<-Acum_Etapa[[j]][,7]
        # Pone los dias Restantes en cada Etapa
        Trig.2[,"DIAS.REST.E1"]<-DRE1
        Trig.2[,"DIAS.REST.E2"]<-DRE2
        Trig.2[,"DIAS.REST.E3"]<-DRE3
        # Datos de diferencia de Precipitacion acumulada
        Trig.2[,c("DIFACUM.DEF.E1")]<--(as.numeric(as.character(TRI2[1,"B"])) - Acum_Etapa[[j]][,2]);
        Trig.2[,c("DIFACUM.DEF.E2")]<--(as.numeric(as.character(TRI2[2,"B"])) - Acum_Etapa[[j]][,4]); 
        Trig.2[,c("DIFACUM.EXC.E2")]<--(as.numeric(as.character(TRI2[3,"A"])) - Acum_Etapa[[j]][,4]); 
        Trig.2[,c("DIFACUM.DEF.E3")]<--(as.numeric(as.character(TRI2[4,"B"])) - Acum_Etapa[[j]][,6]); 
        Trig.2[,c("DIFACUM.EXC.E3")]<--(as.numeric(as.character(TRI2[5,"A"])) - Acum_Etapa[[j]][,6])
        # Agregar Calculo de Dias cosecutivos sin lluvia
        if (exists("DCSP")) {Trig.2[,c("DCSLluvia","DCSPFInicial","DCSPFFinal")]<-DCSP2}
        #######################################################################
        # Definicion de pagos de costo total o parcial
        costoParcial<-round(sum*as.numeric(as.character(Trig.2$SURFACE.HA)),2)  
        costoTotal<-round(1500*as.numeric(as.character(Trig.2$SURFACE.HA)),2)
        if (band==1){
          Trig.2[,"COSTO_TOTAL"]<- costoTotal 
        }
        if (band!=1){
          if (costoParcial>=costoTotal){ Trig.2[,"COSTO_TOTAL"]<-costoTotal
          }
          if (costoParcial <costoTotal){ Trig.2[,"COSTO_TOTAL"]<-costoParcial}
        }
        #######################################################################
        # Proceso de escritura de archivo tipo csv
        ruta.dest<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/TABLA_SEGUIMIENTO"
        if (!file.exists(ruta.dest)){dir.create(ruta.dest)}
        ruta.file<-paste(ruta.dest,"/Seguimiento_año_",aniop,".csv",sep = "")
        # Archivo existe excribira la informacion
        if (file.exists(ruta.file)){write.table(Trig.2,file = ruta.file,append = T,quote = F,sep = ",",na="ND",row.names = F,col.names = F, fileEncoding="latin1")}
        # Archivo no existe crea el archivo a trabajar
        if (!file.exists(ruta.file)){write.table(Trig.2,file = ruta.file,append = F,quote = F,sep = ",",na="ND",row.names = F,col.names = T, fileEncoding="latin1")}
        if (j==1){tabla<-Trig.2}
        if (j> 1){tabla<-rbind(tabla,Trig.2)}
      } # Cierre de for ciclo de revision año x año
      print(paste("Termina proceso de estación:",Est," No:",TRIGGERS$NO.))
    } # Cierre de if proceso de Tlaxcala
    
    # Proceso de conteo de informacion similar
    if (toupper(TRIGGERS$STATE)!="TLAXCALA"){
      Vec.Trigg<-c("T.E1","T.E2.1","T.E2.2","T.E3.1","T.E3.2")
      TRI<-TRIGGERS[,Vec.Trigg]
      Trig.2<-TRIGGERS
      for (j in 1: length(Acum_Etapa)){
        aniop<-Acum_Etapa[[j]][1,1]
        # Ciclo contra la estación a revisar
        # Etapa 1
        Log.DEF.E1<-Acum_Etapa[[j]][,2] < as.numeric(as.character(TRI[,1]))
        nn<-is.na(Acum_Etapa[[j]][,2])
        if (nn=="FALSE"){
          Trig.2[,"DEF.E1"] <-sum(Log.DEF.E1,na.rm = T)
          }
        if (nn=="TRUE"){
          Trig.2[,"DEF.E1"] <-NA
          }
        # Etapa 2
        Log.DEF.E2<-Acum_Etapa[[j]][,4] < as.numeric(as.character(TRI[,2]))
        Log.EXC.E2<-Acum_Etapa[[j]][,4] > as.numeric(as.character(TRI[,3]))
        Trig.2[,"DEF.E2"] <-sum(Log.DEF.E2,na.rm = T)
        Trig.2[,"EXC.E2"] <-sum(Log.EXC.E2,na.rm = T)
        
        # Etapa 3
        Log.DEF.E3<-Acum_Etapa[[j]][,6] < as.numeric(as.character(TRI[,4]))
        Log.EXC.E3<-Acum_Etapa[[j]][,6] > as.numeric(as.character(TRI[,5]))
        Trig.2[,"DEF.E3"] <-sum(Log.DEF.E3,na.rm = T)
        Trig.2[,"EXC.E3"] <-sum(Log.EXC.E3,na.rm = T)
        
        # Total.Siniestros
        Trig.2[,"TOTAL.SIN"]<-sum(Trig.2[,"EXC.E3"],Trig.2[,"DEF.E3"],Trig.2[,"EXC.E2"],Trig.2[,"DEF.E2"],Trig.2[,"DEF.E1"],na.rm = T)
        # Probabilidades
        if (nn=="FALSE"){Trig.2[,"DIFACUM.DEF.E1"]<-Acum_Etapa[[j]][,2] - as.numeric(as.character(TRI[,1]))}
        if (nn=="TRUE"){Trig.2[,"DIFACUM.DEF.E1"]<-NA}
        Trig.2[,"DIFACUM.DEF.E2"]<-Acum_Etapa[[j]][,4] - as.numeric(as.character(TRI[,2]))
        Trig.2[,"DIFACUM.EXC.E2"]<-Acum_Etapa[[j]][,4] - as.numeric(as.character(TRI[,3]))
        Trig.2[,"DIFACUM.DEF.E3"]<-Acum_Etapa[[j]][,6] - as.numeric(as.character(TRI[,4]))
        Trig.2[,"DIFACUM.EXC.E3"]<-Acum_Etapa[[j]][,6] - as.numeric(as.character(TRI[,5]))
        # Escribe el acumulado en cada etapa si existe
        Trig.2[,"ACUM.E1"]<-Acum_Etapa[[j]][,2]
        Trig.2[,"ACUM.E2"]<-Acum_Etapa[[j]][,4]
        Trig.2[,"ACUM.E3"]<-Acum_Etapa[[j]][,6]
        # Escribe el porcentaje de datos en cada etapa
        Trig.2[,"POR.DAT.E1"]<-Acum_Etapa[[j]][,3]
        Trig.2[,"POR.DAT.E2"]<-Acum_Etapa[[j]][,5]
        Trig.2[,"POR.DAT.E3"]<-Acum_Etapa[[j]][,7]
        # Pone los dias Restantes en cada Etapa
        Trig.2[,"DIAS.REST.E1"]<-DRE1
        Trig.2[,"DIAS.REST.E2"]<-DRE2
        Trig.2[,"DIAS.REST.E3"]<-DRE3
        # Conteo de todos los siniestros ocurridos en todas las etapas
        Sum<-colSums(rbind(Log.DEF.E1,Log.DEF.E2,Log.DEF.E3,Log.EXC.E2,Log.EXC.E3),na.rm=T)
        Log.Sum<-(Sum>0) # Vector lógico que obtiene aquellos siniestros que sucedieron en el mismo año
        # Supuesto dado que todos los vectores de comparacion son iguales, es decir, cada vector de comparacion de cultivo
        # contiene los mismo años en cada etapa se pueden considerar que el numero de siniestros en cada año se acumula y toma los valores de 0 a 3 no mas dado
        # que nos se puede siniestrar mas de tres veces por que se tienen 3 etapas con 5 condiciones de deficit y exceso
        # finalmente la suma de Log.Sum es el valor de siniestros por año que sucedieron quitando la situacion de que se tuvieron siniestros posteriores
        ###################################################################
        # Agregar Calculo de Dias cosecutivos sin lluvia
        if (exists("DCSP2")) {Trig.2[,c("DCSLluvia","DCSPFInicial","DCSPFFinal")]<-DCSP2}
        if (!exists("DCSP2")) {Trig.2[,c("DCSLluvia","DCSPFInicial","DCSPFFinal")]<-NA}
        # Calculo de costo total
        Trig.2[,"COSTO_TOTAL"]<-round(1500*as.numeric(as.character(TRIGGERS$SURFACE.HA))*Sum,2)
        # Agregar criterio de dias consecutivos sin lluvia
        Estados<-c("Guanajuato","Michoacán","Puebla","Querétaro")
        Dias_Estado_DCSP<-c(20,20,16,21)
        Log.Edo<-as.character(Trig.2$STATE)==Estados
        PosEst<-grep(TRUE,Log.Edo)
        if (length(PosEst)>0 & !is.na(Trig.2$DCSLluvia)){
          if (Trig.2$DCSLluvia>=Dias_Estado_DCSP[PosEst]){Trig.2[,"COSTO_TOTAL"]<-round(1500*as.numeric(as.character(TRIGGERS$SURFACE.HA)),2)}
        }
        ###################################################################
        # Proceso de escritura de archivo tipo csv
        ruta.dest<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/TABLA_SEGUIMIENTO"
        if (!file.exists(ruta.dest)){dir.create(ruta.dest)}
        ruta.file<-paste(ruta.dest,"/Seguimiento_año_",aniop,".csv",sep = "")
        # Archivo existe excribira la informacion
        if (file.exists(ruta.file)){write.table(Trig.2,file = ruta.file,append = T,quote = F,sep = ",",na="ND",row.names = F,col.names = F, fileEncoding="latin1")}
        # Archivo no existe crea el archivo a trabajar
        if (!file.exists(ruta.file)){write.table(Trig.2,file = ruta.file,append = F,quote = F,sep = ",",na="ND",row.names = F,col.names = T, fileEncoding="latin1") }
        
      } # Termina for ciclo de analizar cada año
      print(paste("Termina proceso de estación:",Est," No:",TRIGGERS$NO.))
    } # Cierra condicion que pregunta por el estado de Tlaxcala
  } # Termina if que significa si la longitud del archivo buscado es mayor a cero eso implica que "EXISTE EL ARCHIVO" 
  
  # En caso de no tener datos disponibles en la simulacion 
  if (sum(FII,na.rm=T)==0){ 
    print(paste("Ninguna Etapa a comenzado. Proceso saltado:",Est,". No:",TRIGGERS$NO.))
    Trig.2<-TRIGGERS
    Vece<-c("DEF.E1","DEF.E2","EXC.E2","DEF.E3","EXC.E3","TOTAL.SIN","DIFACUM.DEF.E1","DIFACUM.DEF.E2","DIFACUM.EXC.E2","DIFACUM.DEF.E3","DIFACUM.EXC.E3","ACUM.E1","ACUM.E2","ACUM.E3","POR.DAT.E1","POR.DAT.E2","POR.DAT.E3","DIAS.REST.E1","DIAS.REST.E2","DIAS.REST.E3","DCSLluvia","DCSPFInicial","DCSPFFinal","COSTO_TOTAL")
    Trig.2[,Vece]<-NA
    if (!file.exists(ruta.dest)){dir.create(ruta.dest)}
    aniop<-format.Date(Trig.2$DateFromP2,format="%Y")
    ruta.dest<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/TABLA_SEGUIMIENTO/"
    ruta.file<-paste(ruta.dest,"/Seguimiento_año_",aniop,".csv",sep = "")
    # Si archivo existe escribe en la siguiente linea
    if (file.exists(ruta.file)){
      write.table(Trig.2,file = ruta.file,append = T,quote = F,sep = ",",na="ND",row.names = F,col.names = F, fileEncoding="latin1")
    }
    # Si archivo no existe crea el archivo
    if (!file.exists(ruta.file)){
      write.table(Trig.2,file = ruta.file,append = F,quote = F,sep = ",",na="ND",row.names = F,col.names = T, fileEncoding="latin1")
    }
    
  } # // Cierre en caso de no encontrar los datos necesarios
  
} ##  Fin de la funcion Calc_Siniestro
Log<-(Triggers$SMN.No=="30008" & Triggers$Cultivo=="Maíz")
Ind<-grep("TRUE", Log, fixed = T)

ruta.dest<-"/home/modelo/Documentos/Documentos/Scripts/Seguimiento_Parametrico/TABLA_SEGUIMIENTO/"
Files.rm<-(Sys.glob(paste(ruta.dest,"*.csv",sep=""),dirmark = T))
file.remove(Files.rm)
# apply(Triggers.1[(Ind+1):nrow(Triggers.1),],1, function(x) Calc_Siniestro_Simula(x, DatosPPP=Files.lanina))
# apply(Triggers.1[(Ind):nrow(Triggers.1),],1, function(x) Calc_Siniestro_Simula(x, DatosPPP=Files.elnino))

lapply(split(Triggers,Triggers$NO.), function(x) Calc_Siniestro_Simula(x, DatosPPP=DatosPrec))




