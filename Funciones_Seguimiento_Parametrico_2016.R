# Grupo de funciones generadas para el seguimiento del Parametrico 2016
# Funciones de Extracción de Acumulados de Etapa cualquiera
################################################################################
# Variables de  apoyo o auxiliares
Cult.Ini.1<-TRIGGERS$DateFromP1;Cult.Ini.2<-TRIGGERS$DateFromP2; Cult.Ini.3<-TRIGGERS$DateFromP3
Cult.Fin.1<-TRIGGERS$DateToP1; Cult.Fin.2<-TRIGGERS$DateToP2; Cult.Fin.3<-TRIGGERS$DateToP3;
# DDatos<-Datos
# Funcion de extraccion de fechas de cultivos
Ext_Etapa<-function(DDatos,Cult.Ini.1,Cult.Fin.1,Cult.Ini.2,Cult.Fin.2,Cult.Ini.3,Cult.Fin.3){
  # Año de analisis
  anio<-format.Date(as.character(DDatos[1,"FECHA"]),format="%Y")
  # FEcha maxima contenida en los registros de la estacion de la base del parametrico
  FechaMax<-max(DDatos$FECHA)
  # Prueba logica de los datos que se calcularan debido a si ha empezado etapa o no
  FIII<-FechaMax>c(Cult.Ini.1,Cult.Ini.2,Cult.Ini.3)
  # Analisis de la Etapa 1 para fechas y vector logico de suma
  if (is.na(FIII[1])){
    # Fechas que se desean extraer
    F.Ini_1<-NA
    F.Fin_1<-NA
    Log.1<-0
  }
  if (!is.na(FIII[1]) & FIII[1] == TRUE){
    # Fechas que se desean extraer
    F.Ini_1<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Ini.1,format="%m")),"-",as.numeric(format.Date(Cult.Ini.1,format="%d")),sep=""))
    F.Fin_1<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Fin.1,format="%m")),"-",as.numeric(format.Date(Cult.Fin.1,format="%d")),sep=""))
    # Vector Logicos de Fechas de la etapa 1
    Log.1<-(DDatos[,"FECHA"]<=F.Fin_1  & DDatos[,"FECHA"]>=F.Ini_1); Log.1<-Log.1[!is.na(Log.1)]
  }
    # Analisis de la Etapa 2 para fechas y vector logico de suma
    F.Ini_2<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Ini.2,format="%m")),"-",as.numeric(format.Date(Cult.Ini.2,format="%d")),sep=""))
    F.Fin_2<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Fin.2,format="%m")),"-",as.numeric(format.Date(Cult.Fin.2,format="%d")),sep=""))
    # Vectores Logicos
    Log.2<-(DDatos[,"FECHA"]<=F.Fin_2  & DDatos[,"FECHA"]>=F.Ini_2); Log.2<-Log.2[!is.na(Log.2)]
    # Analisis de la Etapa 3 para fechas y vector logico de suma
    F.Ini_3<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Ini.3,format="%m")),"-",as.numeric(format.Date(Cult.Ini.3,format="%d")),sep=""))
    F.Fin_3<-as.Date(paste(anio,"-",as.numeric(format.Date(Cult.Fin.3,format="%m")),"-",as.numeric(format.Date(Cult.Fin.3,format="%d")),sep=""))
    # Vector Logico de la etapa 3
    Log.3<-(DDatos[,"FECHA"]<=F.Fin_3  & DDatos[,"FECHA"]>=F.Ini_3); Log.3<-Log.3[!is.na(Log.3)]
    # Generar variables vacias para a mandar a llamar para realizar el calculo de las variables
    PREC.E1<-NA; PREC.E2<-NA;PREC.E3<-NA
    POR.E1<-NA; POR.E2<-NA;POR.E3<-NA
    # Extraccion de etapas de los datos de lluvia
    n.1<-sum(Log.1); n.2<-sum(Log.2) ;n.3<-sum(Log.3)  
    # Condiciones para realizar los acumulados
    # Sin primera Etapa 1
    if (n.1 == 0){
      if (n.2 > 0){
        DDatos2<-DDatos[Log.2,]
        PREC.E2<-sum(DDatos2[,2],na.rm=T)
        DIF_DIAS<-as.numeric(F.Fin_2-F.Ini_2+1)
        POR.E2<-round((sum(Log.2)-sum(is.na(DDatos2[,2])))/DIF_DIAS*100,2)}
      if (n.3 > 0){
        DDatos3<-DDatos[Log.3,]
        PREC.E3<-sum(DDatos3[,2],na.rm=T)
        DIF_DIAS<-as.numeric(F.Fin_3-F.Ini_3+1)
        POR.E3<-round((sum(Log.3)-sum(is.na(DDatos3[,2])))/DIF_DIAS*100,2)
      }} # Cierre de If que no tiene primera Etapa
    # Con primera Etapa 1
    if (n.1 > 0){
      DDatos1<-DDatos[Log.1,]
      PREC.E1<-sum(DDatos1[,2],na.rm=T)
      DIF_DIAS<-as.numeric(F.Fin_1-F.Ini_1+1)
      POR.E1<-round((sum(Log.1)-sum(is.na(DDatos1[,2])))/DIF_DIAS*100,2)
      if (n.2 > 0){
        DDatos2<-DDatos[Log.2,]
        PREC.E2<-sum(DDatos2[,2],na.rm=T)
        DIF_DIAS<-as.numeric(F.Fin_2-F.Ini_2+1)
        POR.E2<-round((sum(Log.2)-sum(is.na(DDatos2[,2])))/DIF_DIAS*100,2)}
      if (n.3 > 0){
        DDatos3<-DDatos[Log.3,]
        PREC.E3<-sum(DDatos3[,2],na.rm=T)
        DIF_DIAS<-as.numeric(F.Fin_3-F.Ini_3+1)
        POR.E3<-round((sum(Log.3)-sum(is.na(DDatos3[,2])))/DIF_DIAS*100,2)
      }} # Cierre de If que no tiene primera Etapa
    # Genera Dataframe a utilizar
    RDatos<-data.frame(ANIO=anio,PREC.E1=PREC.E1,POR.E1=POR.E1,PREC.E2=PREC.E2,POR.E2=POR.E2,PREC.E3=PREC.E3,POR.E3=POR.E3)
    return(RDatos)
  } # Termina la funcion de extraccion etapa
################################################################################
############################################
# Funcion de extraccion de fechas de cultivos
Ext_Etapa2<-function(DDatos,Cult.Ini,Cult.Fin){
  dia1<-as.numeric(format.Date(Cult.Ini,format="%d"))
  mes1<-as.numeric(format.Date(Cult.Ini,format="%m"))
  dia2<-as.numeric(format.Date(Cult.Fin,format="%d"))
  mes2<-as.numeric(format.Date(Cult.Fin,format="%m"))
  n<-names(DDatos);
  names(DDatos)<-c("FECHA",n[2:length(n)])
  anio<-as.numeric(format.Date(DDatos[1,"FECHA"],format="%Y"))
  F.Ini<-as.Date(paste(anio,"-",mes1,"-",dia1,sep=""))
  F.Fin<-as.Date(paste(anio,"-",mes2,"-",dia2,sep=""))
  Log<-DDatos[,"FECHA"]<=F.Fin  & DDatos[,"FECHA"]>=F.Ini
  # Extraccion de etapas de los datos de lluvia
  DDatos1<-DDatos[Log,]
  DDatos1<-DDatos1[!is.na(DDatos1$FECHA),]
  return(DDatos1)               
} # Termina la funcion de extraccion etapa
###################################################################
###################################################################
# Funcion de Revision de dias consecutivos en el arreglo previsto  
# data<-Datos_Etapa2[[1]]
P_DCSLluvia<-function(Datos.Cultivo.Etapa){
  data<-Datos.Cultivo.Etapa
  row.names(data)<-NULL
  dias<-nrow(data)
  m <- ncol(data); 
  f<-numeric(m+1)
  dim(f)<-c(1,m+1); f<-as.data.frame(f)
  anio<-format.Date(data[1,"FECHA"],format="%Y")
  nom.Est<-names(data); vari<-c("DCSLluvia", "FInicial","FFinal")
  # Comienza a recorrer las columnas
  for (g in 2:ncol(data)){
    k <-0; aux<-k
    Per1<-0; Per2<-0
    #print(paste("Fila ",g,sep=""))
    diasNa<-sum(is.na(data[,g]))
    # Revision de  condicion de busqueda de datos NA's si todos son NA's se manda NA de Salida
    if (diasNa==dias){aux<-NA;Per1<-NA;Per2<-NA;
    # print(paste("DCSLluvia: ", aux," Periodo: ",Per1," - ", Per2,sep=""));
    if (g==2){tabla<- data.frame(aux,as.character(Per1),as.character(Per2)); names(tabla)<-vari}
    if (g>2){tabla.1<- data.frame(aux,as.character(Per1),as.character(Per2)); names(tabla.1)<-vari; tabla<-cbind(tabla,tabla.1)}
    next}
    
    # Ciclos de condicionantes para la busqueda de información 
    for(h in 1:nrow(data)){
      # Dato a evaluar
      dd<-data[h,g]
      # Revision de existencia de dato si no existe se revisa a su alrededor 
      if (is.na(dd)){
        # Evaluar el primer termino
        if (h==1){
          # Se checa si el siguiente dato es na o valor debajo de cero
          dd1<-data[h+1,g]
          if (is.na(dd1) | dd1>=0.5){next} # si el siguiente es NA o esta arriba de 0.5 salta al siguiente
          if (dd1<0.5)  {   dd<-0}         # si el valor está abajo de 0.5 se le asigna cero
        }
        # Valores intermedios
        if (h > 1 & h < nrow(data)){
          dd1<-data[h+1,g]; dd2<-data[h-1,g]
          if ((is.na(dd1) & is.na(dd2)) | (dd1 >= 0.5 & dd2 >= 0.5) | (dd1 >= 0.5 & is.na(dd2)) | ((is.na(dd1) & dd2 >= 0.5))){next}
          if ((dd1 < 0.5 | dd2 < 0.5)){dd<-0}
        }
        # Valores finales
        if (h==nrow(data)){
          dd2<-data[h-1,g]
          if (is.na(dd2) | dd2 >= 0.5){next}
          if (dd2<0.5){ dd<-0}
        }
      }
      # Revision del dato en cuestion para su comparacion y extraccion final de los periodos
      if (dd<0.5){
        k<-k+1
        if(k>aux){aux<-k; Per2<-data[h,1]; Per1<-data[h-k+1,1]; #print(paste("Aux: ", aux,sep=""))
        
        #print(paste("Aux: ", aux," Periodo: ",Per1," - ", Per2,sep=""))
        }
      }
      if (dd>=0.5){k<-0}
    }
    # print(paste("DCSLluvia: ", aux,", en el Periodo: ",Per1," - ", Per2,sep=""))
    # print(paste("Valor de k: ",k,sep=""))
    # print(paste("Valor de maxk: ",maxk,sep=""))
    if (g==2){tabla<- data.frame(aux,as.character(Per1),as.character(Per2)); names(tabla)<-vari}
    if (g>2){tabla.1<- data.frame(aux,as.character(Per1),as.character(Per2)); names(tabla.1)<-vari; tabla<-cbind(tabla,tabla.1)}
  }
  return(tabla)
} # Fin de Funcion de conteo de dias consecutivos sin lluvia
###################################################################