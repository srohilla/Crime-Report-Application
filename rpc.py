import logging
logging.basicConfig(level=logging.DEBUG)
from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
from spyne.decorator import srpc
import urllib2
from flask import json
import operator
import collections




class CrimeReportService(ServiceBase):
    
    @srpc(float, float ,float,_returns=Iterable(Unicode))
    def checkcrime(lat, lon,radius):
        command="https://api.spotcrime.com/crimes.json?lat=%s&lon=%s&radius=%s&key=."%(lat,lon,radius)
        response = urllib2.urlopen(command)
        data = json.load(response) 
        listdata= data.get("crimes")
        total_crime=len(listdata)
        event_time_count ={
        "12:01am-3am" : 0,
        "3:01am-6am" : 0,
        "6:01am-9am" : 0,
        "9:01am-12noon" : 0,
        "12:01pm-3pm" : 0,
        "3:01pm-6pm" : 0,
        "6:01pm-9pm" : 0,
        "9:01pm-12midnight" : 0
    } 

        crime_type={}
        streets={}
    
        for item in listdata:
            for key in item:
                if key=='type':
                    if crime_type.has_key(item[key]):
                        crime_type[item[key]]=crime_type[item[key]]+1
                    else:
                        crime_type[str(item[key])]=1 
                elif key=='date':
                     ls=item[key].split(' ', 1 )
                   
                     if "AM" in ls[1]:
                         hours=ls[1].split(':', 1 )
                         if (hours[0]=='12' and hours[1] !='00 AM') or hours[0]=='01' or hours[0]=='02' or (hours[0]=='03' and hours[1]=='00 AM'):
                             event_time_count["12:01am-3am"]=event_time_count["12:01am-3am"]+1
                         elif (hours[0]=='03' and hours[1] !='00 AM') or hours[0]=='04' or hours[0]=='05' or (hours[0]=='06' and hours[1]=='00 AM'):
                             event_time_count["3:01am-6am"]=event_time_count["3:01am-6am"]+1 
                         elif (hours[0]=='06' and hours[1] !='00 AM') or hours[0]=='07' or hours[0]=='08' or (hours[0]=='09' and hours[1]=='00 AM'):
                             event_time_count["6:01am-9am"]=event_time_count["6:01am-9am"]+1
                         elif (hours[0]=='09' and hours[1] !='00 AM') or hours[0]=='10' or hours[0]=='11':
                             event_time_count["9:01am-12noon"]=event_time_count["9:01am-12noon"]+1 
                         elif hours[0]=='12' and hours[1]=='00 AM':
                             event_time_count["9:01pm-12midnight"]=event_time_count["9:01pm-12midnight"]+1 
                     else:
                         hours=ls[1].split(':', 1 )
                         if (hours[0]=='12' and hours[1] !='00 PM') or hours[0]=='01' or hours[0]=='02' or (hours[0]=='03' and hours[1]=='00 PM'):
                             event_time_count["12:01pm-3pm"]=event_time_count["12:01pm-3pm"]+1
                         elif (hours[0]=='03' and hours[1] !='00 PM') or hours[0]=='04' or hours[0]=='05' or (hours[0]=='06' and hours[1]=='00 PM'):
                             event_time_count["3:01pm-6pm"]=event_time_count["3:01pm-6pm"]+1 
                         elif (hours[0]=='06' and hours[1] !='00 PM') or hours[0]=='07' or hours[0]=='08' or (hours[0]=='09' and hours[1]=='00 PM'):
                             event_time_count["6:01pm-9pm"]=event_time_count["6:01pm-9pm"]+1
                         elif (hours[0]=='09' and hours[1] !='00 PM') or hours[0]=='10' or hours[0]=='11':
                             event_time_count["9:01pm-12midnight"]=event_time_count["9:01pm-12midnight"]+1 
                         elif hours[0]=='12' and hours[1]=='00 PM':
                             event_time_count["9:01am-12noon"]=event_time_count["9:01am-12noon"]+1
        
                elif key=='address':
                    item[key]=str(item[key])
                 
                    
                    if " AND " in item[key]:
                        st=item[key].split(' AND ',1)    
                        if streets.has_key(st[0]):
                            streets[st[0]]=streets[st[0]]+1
                        else:
                            streets[st[0]]=1
                        if streets.has_key(st[1]):
                            streets[st[1]]=streets[st[1]]+1
                        else:
                            streets[st[1]]=1
                    elif " BLOCK BLOCK " in item[key]:
                        st=item[key].split(' BLOCK BLOCK ',1)
                         
                        if streets.has_key(st[1]):
                            streets[st[1]]=streets[st[1]]+1
                        else:
                            streets[st[1]]=1
                        
                    elif " BLOCK OF " in item[key]:
                        st=item[key].split(' BLOCK OF ',1)
                       
                        if streets.has_key(st[1]):
                            streets[st[1]]=streets[st[1]]+1
                        else:
                            streets[st[1]]=1
                    elif " BLOCK " in item[key]:
                        st=item[key].split(' BLOCK ',1)
                        
                        if streets.has_key(st[1]):
                            streets[st[1]]=streets[st[1]]+1
                        else:
                            streets[st[1]]=1                

                    elif " & " in item[key]:
                        st=item[key].split(' & ')
                       
                        if streets.has_key(st[1]):
                            streets[st[1]]=streets[st[1]]+1
                        else:
                            streets[st[1]]=1
                          
                        if streets.has_key(st[0]):
                            streets[st[0]]=streets[st[0]]+1
                        else:
                            streets[st[0]]=1    
                    else:
                      
                        if streets.has_key(item[key]):
                            streets[item[key]]=streets[item[key]]+1
                        else:
                            streets[item[key]]=1    
               
        sorted_x = sorted(streets.items(), key=operator.itemgetter(1))
        n=len(sorted_x)
        the_most_dangerous_streets=[]
        for i in range(1,4,1):
            if sorted_x[n-i]!=0:
                the_most_dangerous_streets.append(sorted_x[n-i][0])
     
        key_value_pairs = [("total_crime",total_crime),("the_most_dangerous_streets",the_most_dangerous_streets),("crime_type_count",crime_type),("event_time_count",event_time_count)]
        result = collections.OrderedDict(key_value_pairs)
        
        yield result

application = Application([CrimeReportService],
    tns='spyne.assignment.crimereport',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)
if __name__ == '__main__':

    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()