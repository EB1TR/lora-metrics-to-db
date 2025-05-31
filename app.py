# Libreria estándar ----------------------------------------------------------------------------------------------------
#
import sys
import json
# ----------------------------------------------------------------------------------------------------------------------

# Paquetes instalados --------------------------------------------------------------------------------------------------
#
import mysql.connector
import paho.mqtt.client as mqtt
# ----------------------------------------------------------------------------------------------------------------------

# Importaciones locales ------------------------------------------------------------------------------------------------
#
import settings
# ----------------------------------------------------------------------------------------------------------------------


# Variables de Entorno -------------------------------------------------------------------------------------------------
#
MQTT_TOPIC_IN = settings.Config.MQTT_TOPIC_IN
MQTT_PORT = int(settings.Config.MQTT_PORT)
MQTT_HOST = settings.Config.MQTT_HOST
DBHOST = settings.Config.DB_HOST
DBUSER = settings.Config.DB_USER
DBNAME = settings.Config.DB_NAME
DBPASS = settings.Config.DB_PASS
# ----------------------------------------------------------------------------------------------------------------------

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, clean_session=True)

def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe([(MQTT_TOPIC_IN, 1)])
    print(f"Conectado a MQTT: {MQTT_HOST}:{MQTT_PORT} con el topic: {MQTT_TOPIC_IN}")

def on_message(client, userdata, msg):
    try:
        dato = json.loads(msg.payload.decode('utf-8'))

        sql_cam = "`call`, `fw`,`rssi`,`snr`,`ts`,`igate`, `parser`"
        sql_val = f'"{dato["call"]}", "{dato["fw"]}", {dato["rssi"]}, {dato["snr"]}, "{dato["ts"]}", "{dato["igate"]}", "{dato["parser"]}"'
        sqlq = f'INSERT INTO `syslog` ({sql_cam}) VALUES ({sql_val})'

        my_db = mysql.connector.connect(host=DBHOST, user=DBUSER, passwd=DBPASS, database=DBNAME)

        my_cursor = my_db.cursor()
        my_cursor.execute(sqlq)
        my_db.commit()
        my_cursor.close()
        my_db.close()

    except Exception as e:
        print(e)
        print("Fallo en el INSERT:")
        print(msg.payload)

def do_db():
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    while True:
        try:
            mqtt_client.loop_forever(timeout=1.0)

        except KeyboardInterrupt:
            print("live-to-db: Parando: Usuario")
            mqtt_client.disconnect()
            sys.exit(0)
        except EOFError:
            text = 'EOFError: %s' % EOFError
            print(text)
            print("live-to-db: Parando: EOFError")
            mqtt_client.disconnect()
            sys.exit(0)
        except OSError:
            text = 'OSError: %s' % OSError
            print("live-to-db: Parando: OSError")
            print(text)
            mqtt_client.disconnect()
            sys.exit(0)
        except Exception as e:
            text = 'Excepción general: %s' % e
            print("live-to-db: Parando: General")
            print(text)
            mqtt_client.disconnect()
            sys.exit(0)

if __name__ == '__main__':
    do_db()