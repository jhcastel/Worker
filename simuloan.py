#!/usr/bin/python
# -*- coding: latin-1 -*-

import sys
import thread
import time
import math
import random
import datetime
#import boto
import multiprocessing
#from greenlet import greenlet
#from boto.sqs.message import Message
#from boto.sqs.message import RawMessage
#from dynamodb_mapper.model import DynamoDBModel, autoincrement_int
#from dynamodb_mapper.model import ConnectionBorg
from iron_mq import *
from decimal import *

#class client(DynamoDBModel):
#        __table__=u"client"
#        __hash_key__=u"idclient"
#        __schema__ = {
#               u"idclient": unicode,
#                u"idadmin": int,
#                u"birthdate": unicode,
#               u"loanperiod": int,
#                u"loanpurpose": unicode,
#                u"loanrate": unicode,
#				u"loanamount":unicode,
#                u"status": unicode,
#                u"risk": unicode,
#                u"created": unicode,
#                u"modified": unicode,
#                u"record": set,
#       }
#	__defaults__ ={
#        }

#funcion que consulta a la cola
def leer_cola():
	mq=IronMQ()
	queue=mq.queue("queue")
	dict=queue.get()
	datos=str(dict)
	cad=datos.split("u")
	msg=convertir(cad[3])
	msgid=convertir(cad[10])
	
def convertir(mensaje):
	aux1=mensaje.split(",")
	aux2=aux1[0].split("'")
	msg=str(aux2[1])
	return msg
	

def elimina_msj(id):
	mq=IronMQ()
	queue=mq.queue("queue")
	queue.delete(id)
	

# Funcion para manejar conexion y consultas a la DB
#Funcion que guarda el plan de pagos en la bd y cambia el estado en la bd
		
def modifica_cliente(idclient,datem,record,lamount,risk):
	obj=client().get(unicode(idclient))
	#obj.idclient=unicode(idclient)
	obj.risk=unicode(risk)
	obj.modified=unicode(datem)
	obj.record=set(record)
	obj.loanamount=unicode(lamount)
	obj.status=u"Procesado"
	obj.save()	
	print "Actualizado el registro del cliente"

def genera_plan(msg,id):
	k=25
	fibo(k)
	rsk = random.uniform(1,9.99)
	rs=msg.split("|")
	date_mod = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	lclie = rs[0]
	print "Id Client: "+lclie
	lamount = int(rs[1])
	print "loan amount: "+str(lamount)
	lper =int(rs[2])
	lrate=float(rs[3])
	lrate=lrate/100
	plan=genera_cuotas(lclie,Decimal(str(lamount)),int(lper),Decimal(str(lrate)))
	for i in range(0, len(plan)):
		print plan[i]
	#update bd
	#modifica_cliente(unicode(lclie),unicode(date_mod),set(plan),unicode(lamount),str(round(rsk,2)))
	#elimina mensaje de la cola
	elimina_msj(id)
	print "Mensaje eliminado"

# Funcion serie Fibonacci
def fibo(n):
	result = []
	a,b = 0, 1
	end_time = time.time() + n
	while time.time() < end_time:
		a,b = b,a+b

def cuota_anual(amount,period,rate):
	calc = 0
	aux1 = (1-pow(1+rate,-period))/rate
	calc = amount/aux1
	return calc

def genera_cuotas(cod,am,quo,EAR):
	vector=[]
	h = cuota_anual(Decimal(str(am)),int(quo),Decimal(str(EAR)))
	quot_ini = [h, 0.0, 0.0, 1.0, 0.0]
	quot_ini[3] = Decimal(str(quot_ini[3]))*Decimal(str(am))
	for i in range(0,quo):
		quotes=""
		quot_ini[1] = Decimal(quot_ini[3])*Decimal(EAR)
		quot_ini[2] = Decimal(quot_ini[0])-Decimal(quot_ini[1])
		quot_ini[3] = Decimal(quot_ini[3])-Decimal(quot_ini[2])
		quot_ini[4] = Decimal(str(quot_ini[4]))+Decimal(quot_ini[2])
		#arma la cadena del plan de pagos
		loanquote=int(i+1)
		payment=str(round(quot_ini[0],2))
		interest=str(round(quot_ini[1],2))
		principal=str(round(quot_ini[2],2))
		balance=str(round(quot_ini[3],2))
		amortizedamount=str(round(quot_ini[4],2))
		quotes=str(loanquote)+";"+payment+";"+interest+";"+principal+";"+balance+";"+amortizedamount+","
		vector.append(quotes)
	return vector

# Configuracion de proceso intenso de CPU corre durante k segundos

mq=IronMQ()
queue=mq.queue("queue")
dict=queue.get()
datos=str(dict)
cad=datos.split("u")
msg=convertir(cad[3])
msgid=convertir(cad[10])
genera_plan(msg,msgid)
