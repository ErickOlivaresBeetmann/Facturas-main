# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import date,datetime, timedelta

import pyodbc
import pandas as pd
from bs4 import BeautifulSoup

def crear_conexion_SQL():
    """Establecer la conexión con el servidor SQL."""

    try:

        direccion_servidor = "tcp:beetmann-energy.database.windows.net"
        nombre_bd = "mercados"
        nombre_usuario = "adm"
        password = "MercadosBD20"
        
        cnxn = pyodbc.connect("DRIVER={SQL Server};SERVER="+direccion_servidor+";DATABASE="+nombre_bd+";UID="+nombre_usuario+";PWD="+password)
        print('Conexión a la base de datos creada.')
        return cnxn

    except:
        return print("Error en la conexión con la base de datos")





def main():
	    
	directory = 'files'
	data = {}

	for i, f_name in enumerate(os.listdir(directory)):

		# Obtener archivo de factura en formato XML
		f = open(directory + '/' + f_name,'r', encoding='utf-8')
		xml_data = BeautifulSoup(f.read(),'xml')
	
		
		# Obtener informacion del XML
		fecha = xml_data.find('cfdi:Comprobante')['Fecha']
		folio = xml_data.find('cfdi:Comprobante')['Folio']
		print(folio)
		fuf = xml_data.find('ad:CAB')['CODIGO_FUF']
		participante = xml_data.find('ad:CAB')['ID_PARTIC_CENACE']
		importe_total = xml_data.find('ad:CAB')['IMPORTE_TOTAL']
		UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
		try:
			CFDI_relacionado_UUID =  xml_data.find('cfdi:CfdiRelacionado')['UUID']	
		except Exception:
			CFDI_relacionado_UUID = "NA"
		fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
		fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
		fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")		
		print(fecha_Timbrado)
		



		cnxn=crear_conexion_SQL()
		consulta_sql="""SELECT [Folio]
  FROM [dbo].[Facturas] where folio ='{folio}'
                """

		consulta_sql = consulta_sql.format(folio=folio)
		df = pd.read_sql(consulta_sql, cnxn)

		if df.empty==True:
			print('no encontrado, GUARDANDO FACTURA...')

			cnxn=crear_conexion_SQL()
			cursor=cnxn.cursor()
			query_almacenar="""
							INSERT INTO [dbo].[Facturas] ([Fecha]
		  ,[Folio]
		  ,[Participante]
		  ,[FUF]
		  ,[Importe total]
		        ,[Fecha Timbrado]
      ,[CFDI_relacionado_UUID]
      ,[UUID])
							VALUES ('{fecha}','{folio}','{participante}','{fuf}','{importe_total}','{fecha_Timbrado}','{CFDI_relacionado_UUID}','{UUID}');
							"""
			almacenar=query_almacenar.format(fecha=fecha,folio=folio,participante=participante,fuf=fuf,importe_total=importe_total,fecha_Timbrado=fecha_Timbrado
									,CFDI_relacionado_UUID=CFDI_relacionado_UUID,UUID=UUID)
			
			cursor.execute(almacenar)
			cursor.commit()
			print("BD ACTUALIZADA")

		
			# Metodo alternativo para el almacenamiento de datos
			# data[f_name[:-4]] = {'fecha': fecha, 'folio': folio, 'fuf': fuf, 'participante': participante, 'importe_total': importe_total}

			# Alamacenamiento de datos en un diccionario
			data[str(i)] = {'nombre': f_name[:-4], 'fecha': fecha, 'folio': folio, 'fuf': fuf, 'participante': participante, 'importe_total': importe_total}
		else:
			print('YA SE CUENTA CON ESE ESTA FACTURA')

	# Aqui va el resto del codigo


if __name__ == "__main__":
	main()
