import pandas as pd
import os

os.chdir(r"D:\ANALISTA DE DATOS\PROYECTOS\INDICADORES_BM")

ruta_carpetas = r"D:\ANALISTA DE DATOS\PROYECTOS\INDICADORES_BM\MATERIAL"
carpetas = os.listdir(ruta_carpetas)

# Mapeo de meses en texto a números
meses_dict = {'en': '01', 'fe': '02', 'ma': '03', 'ab': '04', 'my': '05', 
              'jn': '06', 'jl': '07', 'ag': '08', 'se': '09', 'oc': '10', 
              'no': '11', 'di': '12'}
df = {}
datos_consolidados = []

for carpeta in carpetas:
    ruta_carpeta = os.path.join(ruta_carpetas, carpeta)
    
    archivos = os.listdir(ruta_carpeta)
    
    for archivo in archivos:
        ruta_archivo = os.path.join(ruta_carpeta, archivo)
    
        df_mensual = pd.read_excel(ruta_archivo)
        
        
        df_mensual.dropna(thresh = 10 , inplace = True) #Eliminar filas que tengan menos de 10 valores no nulos para mantener solo datos relevantes
        df_mensual.dropna(thresh=15, axis=1, inplace=True) # Eliminar columnas que tengan menos de 15 valores no nulos para descartar aquellas con demasiados valores faltantes
        df_mensual.reset_index(drop=True, inplace=True) # Reiniciar los índices del DataFrame después de eliminar filas, evitando desorden en los índices

        
        df_mensual.columns = df_mensual.iloc[0].astype(str).fillna('').str.replace('\u00A0', ' ') #Convierte la primera fila en nombres de columnas, asegurando que sean strings y eliminando espacios no estándar
        df_mensual = df_mensual[1:].reset_index(drop=True) #Elimina la primera fila original (ya que ahora es el encabezado) y reinicia los índices
        
        df_mensual.columns.values[0] = "Indicadores"  # Reemplazar el nombre de la primera columna si es NaN
        df_mensual.set_index("Indicadores", inplace=True) # Establecer la primera columna como índice

        df_mensual = df_mensual.loc[:, df_mensual.columns.notna() & (df_mensual.columns.astype(str) != 'nan')] # Filtrar y conservar solo las columnas que tienen nombres válidos (no NaN)

        df_mensual = df_mensual[~df_mensual.index.isna() & (df_mensual.index.astype(str) != 'nan') & (df_mensual.index != '')] # Eliminar índices vacíos o NaN

        
        # Limpiar nombres de columnas
        df_mensual.columns = (df_mensual.columns.astype(str).fillna('')
                              .str.replace('\u00A0', ' ')  # Reemplazar espacios no separables
                              .str.replace(r'\s+', ' ', regex=True)  # Reducir múltiples espacios a uno
                              .str.replace(',', '')  # Eliminar comas
                              .str.replace(r'\*', '', regex=True)  # Eliminar asteriscos
                              .str.replace(r'[()]', '', regex=True)  # Eliminar paréntesis
                              .str.replace(r'\s*d/', '', regex=True)  # Eliminar "1/" con su espacio antes
                              .str.replace(r'\d', '', regex=True)
                              .str.replace(r'/', '', regex=True)
                              .str.replace(r'\(.*?\)', '', regex=True) #Elimina cada paréntesis con su contenido

                              
                              .str.replace (r'.*CONTINENTAL.*|'
                                            r'B. BBVA Perú',
                                            r'BBVA', regex = True, case = False)
                              
                              .str.replace(r'.*DE COMERCIO.*',
                                           r'BANCOM', regex = True, case = False)
                              
                              .str.replace(r'MI Banco|'
                                           r'Mibanco',
                                           r'MIBANCO', regex = True)
                              
                              .str.replace(r'INTERBANK',
                                           r'Interbank', regex = True)
                              
                              .str.replace(r'.*CRÉDITO.*|'
                                           r'.*CREDITO.*',
                                           r'BCP', regex=True, case=False)
                              
                              .str.replace(r'.*DE FINANZAS.*',
                                           r'BIF', regex = True, case = False)
                              
                              .str.replace(r'.*SANTANDER.*',
                                           r'SANTANDER', regex = True, case = False)
                              
                              .str.replace(r'Bank of China|'
                                           r'China Perú',
                                           r'BANK OF CHINA', regex = True)
                              
                              .str.replace(r'Azteca Perú|'
                                           r'Alfin Banco',
                                           r'ALFIN BANCO', regex = True)
                              
                              .str.replace(r'\b(Perú|bank)\b', '', regex=True)

                              
                              .str.replace(r'\b(Ripley|Falabella Perú|Interbank|Pichincha|Cencosud|'
                                           r'Scotiabank|Deutsche|Falabella)\b', 
                                           lambda x: x.group().upper(), 
                                           regex=True)

                              
                              .str.replace("B. ", "", regex=False)  # Suponiendo que la segunda columna tiene los datos
                              .str.replace(r'\s*con sucursales en el exterior\s*', '', regex=True, case = False)  # Eliminar la frase
                              
                              .str.strip())  # Eliminar espacios al inicio y final
        
        
        
        df_mensual.index = (df_mensual.index.astype(str).fillna('')
                            .str.replace('\u00A0', ' ')  # Reemplazar espacios no separables
                            .str.replace(r'\(.*?\)', '', regex=True) #Elimina cada paréntesis con su contenido
                            .str.replace(r'\s+', ' ', regex=True)  # Reducir múltiples espacios a uno
                            .str.replace(',', '')  # Eliminar comas
                            .str.replace(r'\*', '', regex=True)  # Eliminar asteriscos
                            .str.replace(r'[()]', '', regex=True)  # Eliminar paréntesis
                            .str.replace(r'\s+\'', "'", regex=True)  # Eliminar espacios antes de la última comilla
                            .str.replace(r'\s*1/', '', regex=True)  # Eliminar "1/" y su espacio antes
                            .str.replace(r'\s*promedio del mes\s*', '', regex=True)  # Eliminar "promedio del mes"
                            .str.replace(r'\s*al\s*\d{1,2}/\d{1,2}/\d{4}', '', regex=True)  # Eliminar fechas dd/mm/aaaa
                            .str.replace(r'\s*al\s*\d{3,4}/\d{4}', '', regex=True)  # Eliminar formatos tipo 307/2009
                            .str.replace(r'\s*/\s*', ' ', regex=True)  # Eliminar "/" con espacios alrededor
                            .str.replace(r'\s*\d+\s*$', '', regex=True)  # Eliminar números al final de la cadena
                            .str.replace(r'\s+\.\s*$', '', regex=True)  # Eliminar punto y espacios finales
                            
                            .str.replace(r'Utilidad Neta Anualizada Activo Promedio|'
                                         r'.*R O A.*',
                                         r'ROA', regex = True)
                            
                            .str.replace(r'Utilidad Neta Anualizada Patrimonio Promedio|'
                                         r'.*R O E.*',
                                         r'ROE', regex = True)
                             .str.strip())  # Eliminar espacios al inicio y final

                    
        
        # Identificar el mes en el nombre del archivo
        mes = next((m for m in meses_dict if m in archivo.lower()), None)       
        
        periodo = pd.to_datetime(f"{carpeta}-{meses_dict[mes]}", format="%Y-%m").strftime("%Y-%m")
        df[periodo] = df_mensual
        
        # Extraer valores
        for entidad in df_mensual.columns:
            valores = df_mensual.reindex(df_mensual.index)[entidad]
            for indicador, valor in valores.items():  
                datos_consolidados.append([entidad, periodo, indicador, valor])
        
#Crear DataFrame consolidado
df_final = pd.DataFrame(datos_consolidados, columns=["Entidad", "Periodo", "Indicador", "Monto"])
df_final["Monto"] = pd.to_numeric(df_final["Monto"], errors="coerce")



df_final.to_excel("INDICADORES_BM_2005-2024.xlsx", sheet_name="IND_BM")

