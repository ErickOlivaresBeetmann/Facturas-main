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



import pdfplumber




def main():
	    
	directory = 'filespagos'
	data = {}

	for i, f_name in enumerate(os.listdir(directory)):
		print(f_name)
		# Obtener archivo de factura en formato XML
		pdf = pdfplumber.open(directory + '/' + f_name)
		page = pdf.pages[0]
		text = page.extract_text()
		table = page.extract_tables()

		# Find 'Fecha de vigencia'
		fecha = table[0][2][1]
		print(fecha)
		#fecha = table[2][1][3]
		fecha = datetime.strptime(fecha,"%d/%m/%Y %H:%M:%S")
		fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
		#fecha= '2022-02-24'


		# Find 'Folio', 'Valor' and 'Importe'
		folios, valores, importes = [], [], []
		for row in table[2][3::]:
			folios.append(row[0])
			valores.append(row[2])
			importes.append(row[8])
			folio = (row[0])
			importe = (row[8])

			cnxn=crear_conexion_SQL()
			consulta_sql="""SELECT [Folio]
	   FROM [dbo].[Pagos_Admin] where folio ='{folio}'
					"""

			consulta_sql = consulta_sql.format(folio=folio)
			df = pd.read_sql(consulta_sql, cnxn)

			if df.empty==True:
				print('no encontrado, GUARDANDO FACTURA...')

				cnxn=crear_conexion_SQL()
				cursor=cnxn.cursor()
				query_almacenar="""
								INSERT INTO [dbo].[Pagos_Admin] (
								[Fecha_vigencia]
      ,[Folio]
      ,[Importe])
								VALUES ('{fecha}','{folio}','{importe}');
								"""
				almacenar=query_almacenar.format(fecha=fecha,folio=folio,importe=importe)
			
				cursor.execute(almacenar)
				cursor.commit()
				print("BD ACTUALIZADA")


			else:
				print('YA SE CUENTA CON ESE ESTA FACTURA')


		pdf.close()



		

	# Aqui va el resto del codigo


if __name__ == "__main__":
	main()

