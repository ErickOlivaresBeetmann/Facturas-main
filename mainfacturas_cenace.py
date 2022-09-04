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
	consumos_CEt_CEo, consumos_CEd = [], []

	for i, f_name in enumerate(os.listdir(directory)):
		print("##########################################")
		print(f_name)
		# Obtener archivo de factura en formato XML
		f = open(directory + '/' + f_name,'r', encoding='utf-8')
		xml_data = BeautifulSoup(f.read(),'xml')
		Tipo_comprobante = xml_data.find('cfdi:Comprobante')['TipoDeComprobante']
		Emisor = xml_data.find('cfdi:Emisor')['Nombre']
		UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
		print(Emisor)
		print(Tipo_comprobante)
		if Emisor == "BTMNN DE MEXICO S A P I DE CV":
			if Tipo_comprobante == "I":
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				folio = xml_data.find('cfdi:Comprobante')['Folio']
				try:
					CFDI_relacionado_UUID =  xml_data.find('cfdi:CfdiRelacionado')['UUID']	
				except Exception:
					CFDI_relacionado_UUID = "NA"
				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']				
				subtotal = xml_data.find('cfdi:Comprobante')['SubTotal']				
				total = xml_data.find('cfdi:Comprobante')['Total']

				cnxn=crear_conexion_SQL()
				consulta_sql="""SELECT [UUID]
		  FROM [dbo].[CFDI_Emitidos_BTMNN] where [UUID] ='{UUID}'
						"""

				consulta_sql = consulta_sql.format(UUID=UUID)
				df = pd.read_sql(consulta_sql, cnxn)

				if df.empty==True:


					cnxn=crear_conexion_SQL()
					cursor=cnxn.cursor()
					query_almacenar="""
									INSERT INTO [dbo].[CFDI_Emitidos_BTMNN] ([Tipo_Comprobante]
			  ,[Fecha]
			  ,[CFDI_relacionado_UUID]
			  ,[Folio]
			  ,[Fecha Timbrado]
			  ,[UUID]
			  ,[Subtotal]
			  ,[Total])
									VALUES ('{Tipo_comprobante}','{fecha}','{CFDI_relacionado_UUID}','{folio}','{fecha_Timbrado}',
									'{UUID}','{subtotal}','{total}');
									"""
					almacenar=query_almacenar.format(Tipo_comprobante=Tipo_comprobante,
											fecha=fecha,
											CFDI_relacionado_UUID=CFDI_relacionado_UUID,
											folio=folio,
											fecha_Timbrado=fecha_Timbrado,
											UUID=UUID,
											subtotal=subtotal,
											total=total)
					print("BD ACTUALIZADA")
					cursor.execute(almacenar)
					cursor.commit()

				else:
					print('YA SE CUENTA CON ESE ESTA FACTURA')
	
			elif Tipo_comprobante == "E":
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']	
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				folio = xml_data.find('cfdi:Comprobante')['Folio']
				try:
					CFDI_relacionado_UUID =  xml_data.find('cfdi:CfdiRelacionado')['UUID']	
				except Exception:
					CFDI_relacionado_UUID = "NA"				
				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']				
				subtotal = xml_data.find('cfdi:Comprobante')['SubTotal']				
				total = xml_data.find('cfdi:Comprobante')['Total']

				cnxn=crear_conexion_SQL()
				consulta_sql="""SELECT [UUID]
		  FROM [dbo].[CFDI_Emitidos_BTMNN] where [UUID] ='{UUID}'
						"""

				consulta_sql = consulta_sql.format(UUID=UUID)
				df = pd.read_sql(consulta_sql, cnxn)

				if df.empty==True:

					cnxn=crear_conexion_SQL()
					cursor=cnxn.cursor()
					query_almacenar="""
									INSERT INTO [dbo].[CFDI_Emitidos_BTMNN] ([Tipo_Comprobante]
			  ,[Fecha]
			  ,[CFDI_relacionado_UUID]
			  ,[Folio]
			  ,[Fecha Timbrado]
			  ,[UUID]
			  ,[Subtotal]
			  ,[Total])
									VALUES ('{Tipo_comprobante}','{fecha}','{CFDI_relacionado_UUID}','{folio}','{fecha_Timbrado}',
									'{UUID}','{subtotal}','{total}');
									"""
					almacenar=query_almacenar.format(Tipo_comprobante=Tipo_comprobante,
											fecha=fecha,
											CFDI_relacionado_UUID=CFDI_relacionado_UUID,
											folio=folio,
											fecha_Timbrado=fecha_Timbrado,
											UUID=UUID,
											subtotal=subtotal,
											total=total)
					print("BD ACTUALIZADA")
					cursor.execute(almacenar)
					cursor.commit()

				else:
					print('YA SE CUENTA CON ESE ESTA FACTURA')

			elif Tipo_comprobante == "P":
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				folio = xml_data.find('cfdi:Comprobante')['Folio']

				fecha_pago = xml_data.find('pago10:Pago')['FechaPago']
				fecha_pago = datetime.strptime(fecha_pago,"%Y-%m-%dT%H:%M:%S")
				fecha_pago= fecha_pago.strftime("%Y-%m-%d %H:%M:%S")

				pagos = xml_data.find('cfdi:Complemento')

				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
				if pagos:
					pago = pagos.find_all("pago10:DoctoRelacionado")
					
					for pag in pago:
						folio_relacionado = pag["Folio"]
						ImpPagado = pag["ImpPagado"]
						ImpSaldoAnt = pag["ImpSaldoAnt"]

						cnxn=crear_conexion_SQL()
						consulta_sql="""SELECT [Folio_Relacionado]
				  FROM [dbo].[Complemento_pagos] where [Folio_Relacionado] ='{folio_relacionado}'
								"""

						consulta_sql = consulta_sql.format(folio_relacionado=folio_relacionado)
						df = pd.read_sql(consulta_sql, cnxn)

						if df.empty==True:

							cnxn=crear_conexion_SQL()
							cursor=cnxn.cursor()
							query_almacenar="""
											INSERT INTO [dbo].[Complemento_pagos] ([Fecha]
											,[Folio]
											,[Fecha Pago]
											,[Folio_Relacionado]
											,[Imp_Pagado]
											,[Imp_Saldo_Ant]
											,[Fecha Timbrado]
											,[UUID])
											VALUES ('{fecha}','{folio}','{fecha_pago}','{folio_relacionado}',
											'{ImpPagado}','{ImpSaldoAnt}','{fecha_Timbrado}','{UUID}');
											"""
							almacenar=query_almacenar.format(fecha=fecha,
													folio=folio,
													fecha_pago=fecha_pago,
													folio_relacionado=folio_relacionado,
													ImpPagado=ImpPagado,
													ImpSaldoAnt=ImpSaldoAnt,
													fecha_Timbrado=fecha_Timbrado,
													UUID=UUID)
							print("BD ACTUALIZADA")
							cursor.execute(almacenar)
							cursor.commit()

						else:
							print('YA SE CUENTA CON ESE ESTA FACTURA')


		elif Emisor == "Centro Nacional de Control de Energia":
			if Tipo_comprobante == "P":
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				try:
					folio = xml_data.find('cfdi:Comprobante')['Folio']
				except:
					folio = 0

				fecha_pago = xml_data.find('pago10:Pago')['FechaPago']
				fecha_pago = datetime.strptime(fecha_pago,"%Y-%m-%dT%H:%M:%S")
				fecha_pago= fecha_pago.strftime("%Y-%m-%d %H:%M:%S")

				pagos = xml_data.find('cfdi:Complemento')

				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
				if pagos:
					pago = pagos.find_all("pago10:DoctoRelacionado")
					
					for pag in pago:
						folio_relacionado = pag["IdDocumento"]
						ImpPagado = pag["ImpPagado"]
						ImpSaldoAnt = pag["ImpSaldoAnt"]

						cnxn=crear_conexion_SQL()
						consulta_sql="""SELECT [UUID_relacionado]
				  FROM [dbo].[Complemento_pagos] where [UUID_relacionado] ='{folio_relacionado}'
								"""

						consulta_sql = consulta_sql.format(folio_relacionado=folio_relacionado)
						df = pd.read_sql(consulta_sql, cnxn)

						if df.empty==True:

							cnxn=crear_conexion_SQL()
							cursor=cnxn.cursor()
							query_almacenar="""
											INSERT INTO [dbo].[Complemento_pagos] ([Fecha]
											,[Folio]
											,[Fecha Pago]
											,[UUID_relacionado]
											,[Imp_Pagado]
											,[Imp_Saldo_Ant]
											,[Fecha Timbrado]
											,[UUID])
											VALUES ('{fecha}','{folio}','{fecha_pago}','{folio_relacionado}',
											'{ImpPagado}','{ImpSaldoAnt}','{fecha_Timbrado}','{UUID}');
											"""
							almacenar=query_almacenar.format(fecha=fecha,
													folio=folio,
													fecha_pago=fecha_pago,
													folio_relacionado=folio_relacionado,
													ImpPagado=ImpPagado,
													ImpSaldoAnt=ImpSaldoAnt,
													fecha_Timbrado=fecha_Timbrado,
													UUID=UUID)
							print("BD ACTUALIZADA")
							cursor.execute(almacenar)
							cursor.commit()

						else:
							print('YA SE CUENTA CON ESE ESTA FACTURA')
				
				

			else:
				FUF = xml_data.find('cfdi:Concepto')['NoIdentificacion']
				Tipo_comprobante = xml_data.find('cfdi:Comprobante')['TipoDeComprobante']
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				folio = xml_data.find('cfdi:Comprobante')['Folio']
				try:
					CFDI_relacionado_UUID =  xml_data.find('cfdi:CfdiRelacionado')['UUID']	
				except Exception:
					CFDI_relacionado_UUID = "NA"
				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")			
				subtotal = xml_data.find('cfdi:Comprobante')['SubTotal']				
				total = xml_data.find('cfdi:Comprobante')['Total']
			


				cnxn=crear_conexion_SQL()
				consulta_sql="""SELECT [UUID]
		  FROM [dbo].[CFDI_Emitidos_CENACE] where [UUID] ='{UUID}'
						"""

				consulta_sql = consulta_sql.format(UUID=UUID)
				df = pd.read_sql(consulta_sql, cnxn)

				if df.empty==True:

					cnxn=crear_conexion_SQL()
					cursor=cnxn.cursor()
					query_almacenar="""
									INSERT INTO [dbo].[CFDI_Emitidos_CENACE] ([Tipo_Comprobante]
			  ,[Fecha]
			  ,[CFDI_relacionado_UUID]
			  ,[Folio]
			  ,[Fecha Timbrado]
			  ,[UUID]
			  ,[FUF]
			  ,[Subtotal]
			  ,[Total])
									VALUES ('{Tipo_comprobante}','{fecha}','{CFDI_relacionado_UUID}','{folio}','{fecha_Timbrado}',
									'{UUID}','{FUF}','{subtotal}','{total}');
									"""
					almacenar=query_almacenar.format(Tipo_comprobante=Tipo_comprobante,
											fecha=fecha,
											CFDI_relacionado_UUID=CFDI_relacionado_UUID,
											folio=folio,
											fecha_Timbrado=fecha_Timbrado,
											UUID=UUID,
											FUF=FUF,
											subtotal=subtotal,
											total=total)
					print("BD ACTUALIZADA")
					cursor.execute(almacenar)
					cursor.commit()

				else:
					print('YA SE CUENTA CON ESE ESTA FACTURA')
		elif Emisor == "Centro Nacional de Control de Energía":
			if Tipo_comprobante == "P":
				fecha = xml_data.find('cfdi:Comprobante')['Fecha']
				fecha = datetime.strptime(fecha,"%Y-%m-%dT%H:%M:%S")
				fecha= fecha.strftime("%Y-%m-%d %H:%M:%S")
				folio = xml_data.find('cfdi:Comprobante')['Folio']

				fecha_pago = xml_data.find('pago10:Pago')['FechaPago']
				fecha_pago = datetime.strptime(fecha_pago,"%Y-%m-%dT%H:%M:%S")
				fecha_pago= fecha_pago.strftime("%Y-%m-%d %H:%M:%S")

				pagos = xml_data.find('cfdi:Complemento')

				fecha_Timbrado = xml_data.find('tfd:TimbreFiscalDigital')['FechaTimbrado']	
				fecha_Timbrado = datetime.strptime(fecha_Timbrado,"%Y-%m-%dT%H:%M:%S")
				fecha_Timbrado= fecha_Timbrado.strftime("%Y-%m-%d %H:%M:%S")
				UUID = xml_data.find('tfd:TimbreFiscalDigital')['UUID']	
				if pagos:
					pago = pagos.find_all("pago10:DoctoRelacionado")
					
					for pag in pago:
						folio_relacionado = pag["IdDocumento"]
						ImpPagado = pag["ImpPagado"]
						ImpSaldoAnt = pag["ImpSaldoAnt"]

						cnxn=crear_conexion_SQL()
						consulta_sql="""SELECT [UUID_relacionado]
				  FROM [dbo].[Complemento_pagos] where [UUID_relacionado] ='{folio_relacionado}'
								"""

						consulta_sql = consulta_sql.format(folio_relacionado=folio_relacionado)
						df = pd.read_sql(consulta_sql, cnxn)

						if df.empty==True:

							cnxn=crear_conexion_SQL()
							cursor=cnxn.cursor()
							query_almacenar="""
											INSERT INTO [dbo].[Complemento_pagos] ([Fecha]
											,[Folio]
											,[Fecha Pago]
											,[UUID_relacionado]
											,[Imp_Pagado]
											,[Imp_Saldo_Ant]
											,[Fecha Timbrado]
											,[UUID])
											VALUES ('{fecha}','{folio}','{fecha_pago}','{folio_relacionado}',
											'{ImpPagado}','{ImpSaldoAnt}','{fecha_Timbrado}','{UUID}');
											"""
							almacenar=query_almacenar.format(fecha=fecha,
													folio=folio,
													fecha_pago=fecha_pago,
													folio_relacionado=folio_relacionado,
													ImpPagado=ImpPagado,
													ImpSaldoAnt=ImpSaldoAnt,
													fecha_Timbrado=fecha_Timbrado,
													UUID=UUID)
							print("BD ACTUALIZADA")
							cursor.execute(almacenar)
							cursor.commit()

						else:
							print('YA SE CUENTA CON ESE ESTA FACTURA')
	
			else:
				print('#######ERROR#########')
				



		# Obtener informacion del XML
		#fecha = xml_data.find('cfdi:Comprobante')['Fecha']

		



	
	# Aqui va el resto del codigo


if __name__ == "__main__":
	main()
