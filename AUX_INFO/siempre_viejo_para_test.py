import logging
import logging.handlers as handlers
import random
import time
import os
import sqlite3
import serial
import socket
import requests
import json
import ast
import datetime
from concurrent import futures
from persistqueue import FIFOSQLiteQueue as q_persistente  # Para crear pila persistente en disco
from pymodbus.client.sync import ModbusSerialClient as MBus_rtu  # Crea clientes modbus
from pymodbus.client.sync import ModbusTcpClient as MBus_TCP
from rd_wr_DB import read_from_db, update_to_db, create_to_db, drop_db

# if os.getcwd() != 'siempre':
#     os.chdir('./siempre')
work_dir=os.getcwd()
# print(work_dir)

# Globales:
DB =  'db.sqlite3'
test_internet = False
test_aveva = False

logger = logging.getLogger('siempre.Log')
logger.setLevel(logging.INFO)
logHandler= handlers.RotatingFileHandler('siempre.log',maxBytes=5*1024*1024, backupCount=5)
logHandler.setLevel(logging.INFO)
logger.addHandler(logHandler)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)        
logger.info('-------------Inicialización Siempre-------------')
logger.info(f'Directorio de trabajo: {work_dir}')

#---------------------------------------------------------------------------------------

def load_initial_configuration():
    """Carga la configuración inicial del software"""
    INITIAL_QUERY = 'SELECT * FROM equipos_equipo'
    initial_configuration = read_from_db(DB,INITIAL_QUERY)                
    return initial_configuration

def set_DB (initial_configuration):
    query_vaciar_interno = f"DELETE FROM equipos_interno WHERE var_name > 0"
    update_to_db(DB,query_vaciar_interno)
    
    for each in initial_configuration:
        internal_name = each['internal_name']
        control = each['control']
        engine_model = each['engine_model']
        voltaje = each['voltaje']
        
        ################### DB process ####################       
        db_query_drop = f'DROP TABLE IF EXISTS {internal_name}'
        drop_db(DB, db_query_drop)
        
        db_query_create= f'CREATE TABLE "{internal_name}" ("internal_name" varchar(50), "var_name" varchar(30), "valor_output" varchar(30), "minimo" real, "maximo" real)'
        create_to_db(DB, db_query_create)
        
        db_query_insert = f'INSERT INTO {internal_name} (var_name, minimo, maximo) SELECT var_name, minimo, maximo FROM equipos_out_var WHERE engine_model = "{engine_model}" AND control = "{control}" AND voltaje = "{voltaje}"'
        update_to_db(DB,db_query_insert)
        
        db_query_insert2 = f"INSERT INTO {internal_name} (var_name) VALUES ('comm_socket'), ('comm_test_socket'), ('comm_device_test')"
        create_to_db(DB,db_query_insert2)
        
        db_query_update= f"UPDATE {internal_name} SET internal_name = '{internal_name}'"
        update_to_db(DB, db_query_update)
        
        db_query_insert3= f"INSERT INTO equipos_interno (internal_name, var_name, valor_output, minimo, maximo) SELECT * FROM {each['internal_name']}"
        update_to_db(DB, db_query_insert3)
        
        db_query = f"DROP TABLE IF EXISTS {internal_name}"
        drop_db(DB, db_query)
  
def update_global_status_values(internet, aveva): 
    
    update_db_query=f"""UPDATE equipos_status_var SET internet_connection = {internet}, aveva_connection = {aveva}"""
    update_to_db(DB,update_db_query)
    
def internet_test():
    IP = '8.8.8.8'
    URL = 'https://online.wonderware.com' #'https://online.wonderware.com/apis/upload/datasource'
      
    while True:            
        test_internet = chk_internet_socket()
        test_aveva = chk_internet_socket(host=URL, url_or_ip= 'url')
        update_global_status_values(test_internet,test_aveva)
        if test_internet == False or test_aveva == False:
            logger.error (f'Conexión a Internet = {test_internet} -- Conexión a Aveva = {test_aveva}')
        else:
            logger.info (f'Conexión a Internet = {test_internet} -- Conexión a Aveva = {test_aveva}')
        time.sleep(60)

def chk_internet_socket(host= '8.8.8.8' ,url_or_ip = 'ip' , timeout=10):
    """ Verifica si existe conexion con el host.
    ...
    
    Args
    ----
    host (str, optional):
        Puede ser una IP ejem.: 1.1.1.1 o una url. Default: '8.8.8.8'.
    url_or_ip (str, optional):
        Definir: "url" o "ip". Default: 'ip'.
    timeout (int, optional):
        Default: 10.

    Returns
    -------
    Boolean:
        Retorna si hay conexion al host.
    """
    try:
        if url_or_ip == 'ip':
            port=53
            socket.setdefaulttimeout(timeout)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
            return True
        else:            
            request = requests.get(host, timeout=timeout) 
            return True
    except socket.error as ex:
        return False
    except (requests. ConnectionError, requests. Timeout) as exception:
        return False

def crear_conector(device):
        """Aqui se crea la conexion con cada puerto"""
        communication_mode = device['communication_mode']
        internal_name = device['internal_name']
        comm_socket=False
        test_socket=False         
        try:
            if communication_mode == 'rtu':               
                modbus = MBus_rtu(method='rtu',port=device['port'],baudrate=int(device['baudrate']),databits=device['databits'],parity=device['parity'],stopbits=device['stopbits'])
                if modbus:
                    comm_socket=True
                    logger.info(f"Se creo conector RTU con dispositivo: {internal_name} en purto: {device['port']}")   
                    socket = verificacion_nodo(modbus, internal_name)
                if socket: 
                    test_socket = True

            if communication_mode == 'TCP':
                modbus = MBus_TCP(method='tcp', host=device['IP'], port=device['port'])
                if modbus:
                    comm_socket=True
                    logger.info(f"Se creo conector TCP con dispositivo: {internal_name} en la IP: {device['IP']}")   
                    socket = verificacion_nodo(modbus, internal_name)
                if socket: test_socket = True
                
            if communication_mode == 'RS232':
                port = device['port']
                baudrate = int(device['baudrate'])
                databits = device['databits']
                parity = device['parity']
                stopbits = int(device['stopbits'])                
                conector_RS232 = serial.Serial(port=port, baudrate=baudrate, bytesize=databits, parity=parity, stopbits=stopbits)
                if conector_RS232.is_open == True:
                    logger.info(f'Se creo conector serie con puerto: {port}')
                    socket=conector_RS232
                    comm_socket=True
                    test_socket=True
                else:
                    socket=None
                    test_socket = False
                    
            # Falta implementar DIESEL                                        
        except Exception as e: 
            logger.error('Modulo: "crear_conector" - ',e)
            comm_socket=False
            test_socket=False
            
    
        finally:
            query_socket = f"UPDATE 'equipos_interno' SET valor_output = {comm_socket} WHERE var_name = 'comm_socket' AND internal_name = '{internal_name}'"
            query_test_socket = f"UPDATE 'equipos_interno' SET valor_output = {test_socket} WHERE var_name = 'comm_test_socket' AND internal_name = '{internal_name}'"
            update_to_db(DB,query_socket)
            update_to_db(DB,query_test_socket)
            return socket
                    
def verificacion_nodo(socket, internal_name):
    """Aqui se verifica la comunicación con cada nodo. Una conexion por puerto."""
    intentos=0
    try:
        while intentos < 11:    #Verifica puerto Abierto
            if intentos < 10:
                socket.connect()
                if socket.socket:           # Intenta conexión con puerto modbus
                    break
                else:
                    logger.warning(f'Modbus en "{internal_name}" sin conexion. Intento de conexión: {intentos}')
                    time.sleep(0.2)
            intentos += 1

        if not socket.socket:
            socket.close()
            logger.error(f'Sin conexion Modbus en "{internal_name}" - Fuera de servicio')
        else:
            logger.info(f'Conexion Modbus con "{internal_name}" - OK')
          
    except Exception as e_test: 
        logger.error(f"{e_test}")
        socket=None
    finally:
        return socket
        
def control_communication_test(self, sck):
    """Verifica la comunicación con cada control y guarda el estado en la base de datos."""
    
    result_comm_test = False
    intentos=0
    while intentos < 15:
        try:
            test_lectura= sck.read_holding_registers(address=1600,count=1,unit=int(self.slave))
            if test_lectura.function_code == 3:
                    logger.info(f"Conexión con {self.internal_name} establecida -- OK. Valor: {test_lectura.registers[0]}")
                    result_comm_test = True
                    break            
        except Exception as e_test_control:
            result_comm_test = False
            logger.error(f"Error de comunicación con control: {self.internal_name} --> {e_test_control}")
        finally:
            intentos+=1
    if result_comm_test == False: logger.error(f"Sin conexion con control de {self.internal_name} --> Fuera de servicio")
    
    query_comm_test = f"UPDATE 'equipos_interno' SET valor_output = {result_comm_test} WHERE var_name = 'comm_device_test' AND internal_name = '{self.internal_name}'"
    update_to_db(DB,query_comm_test)
    
    return result_comm_test

class lectura_equipo():
    
    """Esta clase crea un proceso por cada equipo"""   
    def __init__(self, device):        
        self.internal_name = device['internal_name']
        self.communication_mode = device['communication_mode']
        self.port = device['port']
        self.slave = device['slave']
        self.pila = q_persistente(path=f"./buffer/{self.internal_name}",multithreading=True)
        self.engine_model = device['engine_model']
        self.control = device['control']
        self.token = device['token']
        self.voltaje = device['voltaje']
       
        # carga variables de entrada:                  
        vars_input_query = f"SELECT var_name, address, bit, multiplicador, signo, tratamiento FROM equipos_in_var WHERE engine_model = '{self.engine_model}' AND control = '{self.control}'"
        self.in_vars = read_from_db(DB, vars_input_query)
  
        # carga variables de salida:                  
        vars_output_query = f"SELECT * FROM equipos_interno WHERE internal_name = '{self.internal_name}'"
        self.out_vars = read_from_db(DB, vars_output_query)
       
        #configura el modo de lectura modbus (esto disminuye el tiempo de lectura desde el control):
        self.mode = self.load_mode()
        
        #parametros iniciales:
        self.read_list=[]
        status_send = True
        
        ###########################################################################
        #Loop infinito de conexion y reconexion:
        while True:
            ###########################################################################
            #Loop infinito de envio de datos:
            read_times = 0
            read_alarmas = False
            try:
                socket = crear_conector(device)
                # test_device_comm()
                comm_test = control_communication_test(self, socket)
                if comm_test == False:
                    continue
                else:
                    while socket.socket:
                    
                        if read_times >= 10:
                            status_send = self.send_read_list(read_list = self.read_list)
                            if status_send == False:
                                self.save_in_buffer(read_list=self.read_list)
                            read_times = 0
                            self.read_list=[]
                        else:
                            if read_times >= 9: read_alarmas = True
                            lectura = self.read_device(socket = socket, read_alarmas= read_alarmas)
                            self.send_to_db(lectura)
                            self.read_list.append(lectura)                               
                            read_times +=1
            except Exception as ex:
                logger.error(f"Falla en modulo principal de lectura. Interno {self.internal_name}.",ex) 

            logger.error (f"Falla de conexión de socket en {self.internal_name}")
            time.sleep(10) # Espera para intentar reconexion
            
    def read_device(self, socket, read_alarmas = False):
        read_device_dict = {}
        
        aux_apendice=[]
        aux_list_keys=[]
        aux_vars_leidas={}

        try:
            for key, rango in self.mode.items():
                read_registers=socket.read_holding_registers(key-1,rango,unit=int(self.slave))
                             
                aux_apendice.extend(read_registers.registers)
                for i in range(key,(key+rango)):
                    aux_list_keys.append(i)
                    
            # Acondiciona los datos leidos:
            aux_dict_leido= dict(zip(aux_list_keys,aux_apendice))

            for valor in self.in_vars:
                var = valor['var_name']
                address = valor['address']            
                if address in aux_dict_leido:
                    aux_vars_leidas[var]=aux_dict_leido.get(address)
            read_device_dict = self.calculos(aux_vars_leidas)
            
            # Limita maximos y minimos:        
            read_device_dict = self.limitar_max_min(read_device_dict)
            
            # Insertar variables de estado por equipo:
            query_status = f"SELECT var_name, valor_output FROM 'equipos_interno' WHERE internal_name = '{self.internal_name}' AND var_name LIKE 'comm_%' "
            status_vars = read_from_db(DB,query_status)
            for each in status_vars:
                read_device_dict[each.get('var_name')] = each.get('valor_output')
            
            # Insertar variables de estado globales:
            query_global_status = f"SELECT internet_connection, aveva_connection FROM equipos_status_var"
            global_status_vars = read_from_db(DB,query_global_status)
            read_device_dict["internet_connection"] = global_status_vars[0].get('internet_connection')
            read_device_dict["aveva_connection"] = global_status_vars[0].get('aveva_connection')
                
            # Se agrega TimeStamp:
            hora = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            read_device_dict["DateTime"]= str(hora)
            read_device_dict['Hora_lectura']= str(hora)
            
            # Lectura de alarmas:
            if read_alarmas == True:
                leer_alarmas(aux_dict_leido,self.internal_name,self.control)
                
        except Exception as ex:
            logger.error(f"Modulo Read device {self.internal_name}:\n{ex}")
            return None
        
        return read_device_dict
    
    def send_read_list(self, read_list):
        HEADER = {
        'Content-Type': 'application/json',
        'x-filename': self.internal_name + '.json',
        'Authorization': self.token}

        URL = 'https://online.wonderware.com/apis/upload/datasource' 
      
        try:
            str_read_list = json.dumps(read_list, indent = 4)
            payload = '{"data": ' + str_read_list + '}'
            #Request de envio a Aveva;            
            request_response = requests.request("POST", url=URL, headers=HEADER, data=payload, timeout=3)
            if request_response: response = True           
        except Exception as ex:
                response = False
                test_internet=chk_internet_socket()
                test_aveva = chk_internet_socket(host= URL, url_or_ip='url')
                update_global_status_values(internet = test_internet, aveva=test_aveva)
                logger.error(f"Modulo: send_read_list() -- Error de servidor o token. {self.internal_name}:\n{ex}")
        return response
        
    def save_in_buffer(self, read_list):      
        try:
            pila = q_persistente(path=f"./buffer/{self.internal_name}",multithreading=True)
            pila.put(read_list) # Envia el str a una pila archivada
            pila.task_done()
            logger.warning(f'{self.internal_name} - Error de timeout o conexión con servidor. Se guardan los datos en Buffer.')            
        except Exception as e:
            logger.error('Modulo: guardar_datos():\n{e}')   
        pass    
    
    def calculos(self,var_leidas):
        salida={}
        for variable_entrada in self.in_vars:
            var_name = variable_entrada['var_name']
            
            if variable_entrada['signo'] == 'Signed': # Unisigned a Signed 16bit
                if var_leidas.get(var_name) > 32767:
                    var_leidas[var_name] = var_leidas.get(var_name) - (2 ** 16)
                    
            if variable_entrada['tratamiento'] == 'String': 
                if var_leidas.get('Estado_Genset') == 0: salida['Estado_Genset'] = 'Stopped'
                elif var_leidas.get('Estado_Genset') == 1: salida['Estado_Genset'] = 'Start Pending'
                elif var_leidas.get('Estado_Genset') == 2: salida['Estado_Genset'] = 'Warm Up at IDLE'
                elif var_leidas.get('Estado_Genset') == 3: salida['Estado_Genset'] = 'Running'
                elif var_leidas.get('Estado_Genset') == 4: salida['Estado_Genset'] = 'Cooldown at Rated'
                elif var_leidas.get('Estado_Genset') == 5: 
                    salida['Estado_Genset'] = 'Cooldown as IDLE'
                else: 
                    salida['Estado_Genset'] = 'Unknow'                
                if var_leidas.get('Modo_Control') == 0: salida['Modo_Control'] = 'OFF'
                elif var_leidas.get('Modo_Control') == 1: salida['Modo_Control'] = 'Run/Manual'
                elif var_leidas.get('Modo_Control') == 2: salida['Modo_Control'] = 'Automatico'
                else: salida['Modo_Control'] = 'Unknow'
                
            if variable_entrada['tratamiento'] == 'Bit':
                mascara= 2**(int(variable_entrada.get('bit')))
                if (var_leidas.get(var_name) & mascara) > 0: # Realiza AND con mascara y verifica bit leido.
                    salida[var_name] = 1
                else:
                    salida[var_name] = 0
                    
            if variable_entrada['tratamiento'] == 'Directo': 
                salida[var_name] = var_leidas.get(var_name)

            if variable_entrada['tratamiento'] == 'Temperatura1': 
                salida[var_name] = round(((float(var_leidas.get(var_name)*0.1) - 32) * 0.5555555),2)    # (32 °F − 32) × 5/9 = 0 °C
                
            if variable_entrada['tratamiento'] == 'Temperatura2': 
                salida[var_name] = round(((float(var_leidas.get(var_name)) - 32) * 0.5555555),2)    # (32 °F − 32) × 5/9 = 0 °C

            if variable_entrada['tratamiento'] == 'Knock': 
                k_level = round(((var_leidas.get(var_name) * variable_entrada.get('multiplicador'))- 12.5) ,2)
                if k_level < 0: salida[var_name] = 0
                elif k_level > 100: salida[var_name] = 100
                else: salida[var_name] = k_level
                
            if variable_entrada['tratamiento'] == 'Escalado': 
                salida[var_name] = round((var_leidas.get(var_name) * variable_entrada.get('multiplicador')),2)
               
            if variable_entrada['tratamiento'] == 'Complementario':
                salida['MWh'] = round ((float((var_leidas.get('MWh_a') * 65536) + var_leidas.get('MWh_b'))*0.001),4)
                salida['Horas_Motor'] = round (float((((var_leidas.get('Horas_a') * 65536) + var_leidas.get('Horas_b'))/3600)*0.1),2)

            if variable_entrada['tratamiento'] == 'Horas_GCP2':
                salida['Horas_Motor'] = round ((var_leidas.get('Horas_a') * 0.1) + (var_leidas.get('Horas_b')*1000),2)
                                        
            
        salida['V_Avg_N'] = round ((var_leidas.get('V_L1_N') + var_leidas.get('V_L2_N') + var_leidas.get('V_L3_N'))/3)  # Agregado V_Avg_N solo en GCP2 
           
             
        return salida
    
    def limitar_max_min(self, dict_in):
        salida={}
        try:
        
            for out_var in self.out_vars:
                var_name = out_var['var_name']
                minimo = out_var['minimo']
                maximo = out_var['maximo']
                valor = dict_in.get(var_name)
                
                if minimo != 'NC' or maximo != 'NC':
                    if type(valor) == int:
                        if valor < minimo: valor = int(minimo)
                        if valor > maximo: valor = int(maximo)
                    if type(valor) == float:
                        if valor < minimo: valor = float(minimo)
                        if valor > maximo: valor = float(maximo)
                salida[var_name] = valor
        except Exception as e:
            logger.error('Modulo: "limitar_min_max() - ', e)  
        return salida   
    
    def load_mode(self):
        mode_query = f"SELECT mode FROM equipos_modbus_mode WHERE engine_model = '{self.engine_model}' AND control = '{self.control}'"
        mode = ast.literal_eval(read_from_db(DB, mode_query)[0].get('mode'))
        ###############################################################################
        #### Para el futuro:
        #### Verificar que todas las input var esten dentro del MODE. #################
        ###############################################################################
        return mode
    
    def send_to_db (self, read_data):
        try:
            conn= sqlite3.connect(DB)
            cur = conn.cursor()
            
            for key, value in read_data.items():
                sql_update_query = f"""UPDATE 'equipos_interno' SET valor_output = ? WHERE internal_name = '{self.internal_name}' AND var_name = ?"""
                data = (value, key)
                cur.execute(sql_update_query, data)
    
        except Exception as e:
            logger.error('Modulo: "send_to_db - "', e)

        finally:
            conn.commit()
            conn.close()    

class buffer():
    def __init__(self, device):
        self.internal_name = device['internal_name']
        
        """Carga token desde DB"""
        buffer_query = f"SELECT token FROM equipos_equipo WHERE internal_name = '{self.internal_name}'"
        lectura_db = read_from_db(DB,buffer_query)[0]
        self.token = lectura_db.get('token')
        HEADER = {
                    'Content-Type': 'application/json',
                    'x-filename': self.internal_name + '_buffer.json',
                    'Authorization': self.token}

        URL = 'https://online.wonderware.com/apis/upload/datasource' 
            
        while True:
            pila_buffer = q_persistente(path=f'./buffer/{self.internal_name}',multithreading=True)
            if pila_buffer.qsize() > 0:
                string_pila = pila_buffer.get()
                try:
                    json_pila = json.dumps(string_pila,indent=4)
                    data = '{"data": '+ json_pila + ' }'
                    #Request de envio a Aveva;            
                    request_response = requests.request("POST", url=URL, headers=HEADER, data=data, timeout=3)
                    if request_response:
                        logger.info(f'{self.internal_name} - Enviando Buffer de datos temporales guardados: ... {str(request_response.status_code)}')
                except Exception as ex:
                    test_internet=chk_internet_socket()
                    test_aveva = chk_internet_socket(host= URL, url_or_ip='url')
                    update_global_status_values(internet = test_internet, aveva=test_aveva)
                    logger.error(f"Class buffer() -- {self.internal_name} - Error de Timeout o conexión con servidor en envio de Buffer. Se reintentara...\n{ex}")
                    pila_buffer.put(string_pila)
            pila_buffer.task_done() 
            time.sleep(10)

class leer_alarmas():
    def __init__(self, dicc_leido, internal_name, control):
        #control="GCP2"  # solo test GCP2
        try:
            if control=="PCC3300":
                detalle_alarmas = self.leer_alarmas_PCC3300(dicc_leido = dicc_leido, internal_name = internal_name)                 
            else:
                detalle_alarmas = self.leer_alarmas_GCP2(dicc_leido = dicc_leido, internal_name = internal_name)

            self.insertar_en_db(detalle_alarmas = detalle_alarmas, internal_name =  internal_name)   

        except Exception as e:
            logger.error('Modulo: "leer_alarmas.__init__" - ', e)        

    def leer_alarmas_PCC3300(self, dicc_leido, internal_name):
        try:
            rango=[400,471]
            detalle_alarmas=self.leer_alarmas_bits(dicc_leido = dicc_leido, internal_name = internal_name, rango = rango, control= "PCC3300")
            return detalle_alarmas
        except Exception as e:
            logger.error('Modulo: "leer_alarmas_PCC3300" - ', e)

    def leer_alarmas_GCP2(self, dicc_leido, internal_name ):
        try:                    
            rango_bits_1 = [568,570]
            rango_bits_2 = [584,587]
            rango_words = [572,575]
            detalle_alarmas=[]
            detalle_alarmas=self.leer_alarmas_bits(dicc_leido = dicc_leido, internal_name = internal_name, rango = rango_bits_1, control= "GCP2")
            detalle_alarmas.extend(self.leer_alarmas_bits(dicc_leido = dicc_leido, internal_name = internal_name, rango = rango_bits_2, control= "GCP2"))
            detalle_alarmas.extend(self.leer_alarmas_words(dicc_leido = dicc_leido, internal_name = internal_name, rango = rango_words, control = "GCP2"))
            return detalle_alarmas
        except Exception as e:
            logger.error('Modulo: "leer_alarmas_GCP2" - ', e)

    def leer_alarmas_bits(self, dicc_leido, internal_name, rango, control):
        try:
            inicio_grupo,fin_grupo = rango
            alarmas_leidas={}
            for key in range(inicio_grupo,fin_grupo):
                alarmas_leidas[key]=dicc_leido[key]
            alarmas_activas={}
            for key, valor in alarmas_leidas.items():
                if valor > 0:
                    lista_bits = [int(d) for d in str(bin(valor))[2:]] # convercion binario en lista.
                    alarmas_activas[key]=lista_bits # direccion (ej.: {40400,12})                    
                lista_bits=[]
            self.detalle_alarmas_bits=[]
            for key,lista_de_bits in alarmas_activas.items():
                bit=len(lista_de_bits)-1
                parametros=None
                for bit_en_lista in lista_de_bits:
                    parametros=key,bit
                    if bit_en_lista==1: 
                        self.detalle_alarmas_bits.extend(self.seleccion_lista_alarmas(parametros=parametros, tipo = "bits", control= control))
                    bit=bit-1    
        except Exception as e:
            logger.error('Modulo: "leer_alarmas_bits" - ', e)

        return self.detalle_alarmas_bits

    def leer_alarmas_words(self, dicc_leido, internal_name, control, rango):
        try:
            inicio_grupo,fin_grupo = rango
            alarmas_leidas={}
            detalle_alarmas_words=[]
            for key in range(inicio_grupo,fin_grupo):
                alarmas_leidas[key]=dicc_leido[key]
                if alarmas_leidas[key] > 0:                    
                    parametros=key,alarmas_leidas[key]
                    detalle_alarmas_words.extend(self.seleccion_lista_alarmas(parametros=parametros, tipo = "words", control = "GCP2"))
        except Exception as e:
            logger.error('Modulo: "leer_alarmas_words" - ', e)

        return detalle_alarmas_words 

    def seleccion_lista_alarmas(self, parametros, tipo, control):#engine_model, control):     #Selecciona tipo de configuracion segun tipo de control
        try:
            conn=sqlite3.connect('./db.sqlite3')
            cursor=conn.cursor()
            if tipo == "bits":
                address,bit=parametros
                if control == "PCC3300":
                    sql_select_query= f'SELECT code, name, response FROM alarmas3300 WHERE address={address} and bit={bit}'
                else:
                    sql_select_query= f'SELECT code, name, response FROM alarmas_gcp_bits WHERE address={address} and bit={bit}'

            if tipo == "words":
                address, word= parametros
                sql_select_query= f'SELECT code, name, response FROM alarmas_gcp_palabras WHERE address={address} and word={word}'
            
            cursor.execute(sql_select_query)
            datos= cursor.fetchall()
            return datos       
        except Exception as e:
            logger.error('Modulo: "seleccion_lista_alarmas" - ', e)
        finally:
            conn.commit()
            conn.close()

    def insertar_en_db(self, detalle_alarmas, internal_name):
        try:
            
            conn=sqlite3.connect(DB)
            cursor=conn.cursor()
            data=detalle_alarmas
             
            fecha = datetime.datetime.utcnow().strftime(('%d-%m-%Y'))
            hora = datetime.datetime.utcnow().time().isoformat(timespec='seconds')
            
            for alarma in data:
                code=alarma[0]                
                verificacion_code_query=f"""SELECT EXISTS(SELECT 1 FROM equipos_alarma WHERE code={code} AND internal_name='{internal_name}')"""
                cursor.execute(verificacion_code_query)                
                existe_code=cursor.fetchall()[0][0]
               

                if existe_code==0:  # Si no existe la alarma la agrega al listado
                    list_alarma=list(alarma)
                    list_alarma.extend([internal_name,fecha,hora,1,0,0])
                    alarma=tuple(list_alarma)
                    query="INSERT INTO equipos_alarma (code,name,response,internal_name,fecha,hora,activa,aceptada,a_eliminar) VALUES(?,?,?,?,?,?,?,?,?)"
                    cursor.execute(query,alarma)
                else:
                    aceptada_query=f"""SELECT aceptada FROM equipos_alarma WHERE code={code} AND internal_name='{internal_name}'"""
                    cursor.execute(aceptada_query)
                    esta_aceptada = cursor.fetchall()[0][0]
                    if esta_aceptada == 1:
                        query=f'UPDATE equipos_alarma SET activa=1'
                    else:
                        query=f'UPDATE equipos_alarma SET fecha="{fecha}", hora="{hora}", activa=1 WHERE code={code} AND internal_name="{internal_name}"'
                    cursor.execute(query)

        except Exception as e:
            logger.error('Modulo: "insertar_en_db" - ', e)

        finally:
            conn.commit()
            conn.close()
          
def main():  
    try:
        update_global_status_values(False,False)
        
        #Carga conf. inicial y prepara DB
        initial_config = load_initial_configuration() 
        logger.info('Carga de configuración inicial....')
        set_DB(initial_configuration=initial_config)
        
        with futures.ThreadPoolExecutor() as main_executor:
            future_internet_test = main_executor.submit(internet_test)
            future_buffer = main_executor.map(buffer,initial_config)
            future_lectura = main_executor.map(lectura_equipo,initial_config)
            
            
    except Exception as e: 
        logger.error('Main --- ', e)

if __name__=='__main__':
    main()
 