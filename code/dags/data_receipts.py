from bs4 import BeautifulSoup
import re
import dateutil.parser as parser
from dateutil import tz
from datetime import datetime
import csv
import s3fs
import pickle
from airflow.hooks.base_hook import BaseHook

class data_receipts:

    def __init__(self, business, emlobject, filename, id):
        self.business= business
        self.eml = emlobject
        self.content  =  self.eml["body"][0]["content"]
        self.filename  =  filename
        self.id = id

    def save_as_csv(self, r, b, n = ''):
        aws_connection = BaseHook.get_connection('aws_credentials')
        s3 = s3fs.S3FileSystem(anon=False,  key = aws_connection.login, secret= aws_connection.password)
        s3.read_timeout = 3600
        csv_file = n + self.business + '_receipts.csv'
        keys = r[0].keys()
        with s3.open(b + '/' + self.business  + '/' + csv_file,'w', newline='') as output_file:
            dict_writer = csv.DictWriter( output_file, keys, delimiter=";")
            dict_writer.writeheader()
            dict_writer.writerows(r)   

        print("Dataset of receipts saved as ", csv_file )      

    
    def get_data(self):

        receipts = {}
        items = {}
        content = self.cleanMe(self.content)
        content= content.replace(" ", "").replace("\n", "").replace('MX$','$').replace('$','MX$')

        receipts['subject'] = self.eml["header"]["subject"]
        receipts['userservice'] = ','.join(self.eml["header"]["to"])
        receipts['uber_email'] = self.eml["header"]["from"]
        receipts['date'] = self.toLocalDate(self.eml["header"]["date"])
        receipts['filename'] = self.filename
        receipts['service'] = self.business        

        if self.business == 'eats':                                                
            receipts['amount_Charged'] = self.get_due_service(content, 'AmountCharged', 'MX$', '</td>')
            receipts['total'] = self.get_due_service(content, 'Total', 'MX$', '</span>')
            receipts['subtotal'] = self.get_due_service(content, 'Subtotal', 'MX$', '</td>')
            receipts['delivery_fee'] = self.get_due_service(content, 'DeliveryFee', 'MX$', '</td>')
            receipts['service_Fee'] = self.get_due_service(content, 'ServiceFee', 'MX$', '</td>')
            receipts['change'] = self.get_due_service(content, 'Change', 'MX$', '</td>')                     
            receipts['restaurant'] = ' '.join(re.findall('[A-Z][^A-Z]*', self.get_due_service(content, 'Youorderedfrom', 'MX$', '</td>', True)))
            receipts['picked_up_from'] = self.format_address(self.get_due_service(content, 'Pickedupfrom', '<td>', '</td>'))
            receipts['delivered_to'] = self.format_address(self.get_due_service(content, 'Deliveredto', '<td>', '</td>'))
            receipts['lat_from'] = 0.0
            receipts['long_from'] = 0.0
            receipts['lat_to'] = 0.0
            receipts['long_to'] = 0.0
            receipts['items'] = self.id

            items = self.get_items_from_eats(content, self.id)

            return receipts, items 


        else:
            receipts['amount_charged'] = self.get_due_service(content, 'AmountCharged,Montocobrado', 'MX$', '</td>')
            receipts['total'] = self.get_due_service(content, 'Total', 'MX$', '</span>')
            receipts['subtotal'] = self.get_due_service(content, 'Subtotal', 'MX$', '</td>')
            receipts['booking_fee'] = self.get_due_service(content, 'Cuotadesolicitud,BookingFee', 'MX$', '</td>')
            receipts['government_contribution'] = self.get_due_service(content, 'Contribucióngubernamental,GovernmentContribution', 'MX$', '</td>')
            receipts['wait_time'] = self.get_due_service(content, 'Tiempodeespera', 'MX$', '</td>')
            receipts['trip_fare'] = self.get_due_service(content, 'Tarifa,TripFare', 'MX$', '</td>')
            receipts['discounts'] = self.get_due_service(content, 'Descuentos', 'MX$', '</td>')
            receipts['before_Taxes'] = self.get_due_service(content, 'BeforeTaxes', 'MX$', '</td>')
            receipts['balance'] = self.get_due_service(content, 'Balance', 'MX$', '</td>')
            receipts['time_payment'] = self.get_due_service(content, 'Tiempo', 'MX$', '</td>')
            receipts['distance'] = self.get_due_service(content, 'Distancia', 'MX$', '</td>')
            receipts['unsettled_past_uber_trip'] = self.get_due_service(content, 'YourunsettledpastUbertrip', 'MX$', '</td>')
            receipts['distance_service'] = self.get_distance_service(content, 'AmountCharged,Montocobrado', 'km|')
            from_to = self.get_times_service(content, self.toLocalDate(self.eml["header"]["date"]))
            receipts['time_from_service'] = from_to[0]
            receipts['time_to_service'] = from_to[1]
            addresses = self.get_address(content)
            receipts['from_address'] = self.format_address(addresses[0])
            receipts['to_address'] = self.format_address(addresses[1])
            receipts['lat_from'] = 0.0
            receipts['long_from'] = 0.0
            receipts['lat_to'] = 0.0
            receipts['long_to'] = 0.0

            return receipts


    def cleanMe(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        for p in soup.find_all('span'):
            if 'style' in p.attrs:
                del p.attrs['style']

        for p in soup.find_all('body'):
            if 'style' in p.attrs:
                del p.attrs['style']

        for p in soup.find_all('table'):
            if 'style' in p.attrs:
                del p.attrs['style']

        for p in soup.find_all('table'):
            if 'border' in p.attrs:
                del p.attrs['border']                        

        for p in soup.find_all('div'):
            if 'style' in p.attrs:
                del p.attrs['style']            

        for p in soup.find_all('td'):
            if 'style' in p.attrs:
                del p.attrs['style'] 

        for p in soup.find_all('td'):
            if 'valign' in p.attrs:
                del p.attrs['valign']

        for p in soup.find_all('td'):
            if 'vAlign' in p.attrs:
                del p.attrs['vAlign']            

        for p in soup.find_all('td'):
            if 'align' in p.attrs:
                del p.attrs['align']

        for p in soup.find_all('td'):
            if 'rowSpan' in p.attrs:
                del p.attrs['rowSpan']

        for p in soup.find_all('td'):
            if 'colSpan' in p.attrs:
                del p.attrs['colSpan']            

        for p in soup.find_all('td'):
            if 'class' in p.attrs:
                del p.attrs['class']
        
        return soup.prettify()


    def to24(self, s):
        
        if s[-2:] == "AM" and s.split(':')[0] == "12":
            return "00" + s[2:-2]
            
        elif s[-2:] == "AM":
            return s[:-2]
        
        elif s[-2:] == "PM" and s.split(':')[0] == "12":
            return s[:-2]
            
        else:          
            return str(int(s.split(':')[0]) + 12) + ':' + s[2:5].replace(':','')        

    def get_times_service(self, w, d):

        d_object = datetime.strptime(d[:19],"%Y-%m-%d %H:%M:%S")
        d_object = d_object.replace(second= 0)
        
        tms = re.findall('[012]*[0-9][:][0-9]*[0-9][APap]*[Mm]*', w)        

        for t in range(0, len(tms)):
            tms[t] = tms[t].upper()
            tms[t] = tms[t].replace('AM',' AM').replace('PM',' PM')
            if (('AM' in tms[t]) or ('PM' in tms[t])): 
                nh = self.to24(tms[t])
                tms[t] = str(d_object.replace(hour = int(nh.split(':')[0]), minute= int(nh.split(':')[1])))
            else:
                tms[t] = str(d_object.replace(hour=int(tms[t].split(':')[0]), minute=int(tms[t].split(':')[1])))
        
        return tms


    def get_distance_service(self, w, word1, word2):

        words = word1.split(',')

        inx1 = -1
        for word in words:
            inx1 = w.find(word)
            if inx1 > 0:
                word1 = word
                break

        if inx1 < 0:
            return None

        w = w.replace('kilometers', 'km')
        flag = '</td><td>'

        inx1 = w.find(word1)
        inx2 = w.find(word2, inx1 + (len(word1) - 1))
        d_service = w[inx1:inx2] 
        inx3 =  d_service.rfind(flag)

        return d_service[inx3 + len(flag):]

    def format_item(self, s):

        s = s.replace('+', ' ').replace('®', ' ')    
        s = re.sub(r"(\w)([A-Z])", r"\1 \2", s)
        s= re.sub(r"([0-9]+(\.[0-9]+)?)",r" \1 ", s)
        s= " ".join(w.capitalize() for w in s.split())


        return s.strip()             


    def get_items_from_eats(self, s, id):

        items = re.findall('<div>[a-zA-Z0-9]</div></td><td>.*?</td></tr></tbody></table></td><td>.*?</td></tr></tbody></table>', s)
        

        results = []
        for i in range(0, len(items)):
            result = {}
            items[i] = items[i].replace('<div>', '').replace('</div></td><td>','/').replace('</td></tr></tbody></table>', '/').replace('</td><td>','')[0:-1]
            items[i] = items[i].replace('MX$','')
            sections = items[i].split('/')
            result['item'] = self.format_item(sections[1])
            result['qty'] = sections[0]
            result['cost'] = sections[2]
            result['id'] = id
            results.append(result)

        return results

    def get_address(self, w):

        result = []
        tms = re.findall('[012]*[0-9][:][0-9]*[0-9][APap]*[Mm]*', w)

        for t in tms:
            res = re.search( t + '</td></tr><tr><td>(.*?)</td></tr></table>', w)
            result.append(res.group(1))

        return result        
            

    def format_address(self, s):

        if s is None:
            return ''

        pieces = s.split(',')
        address = []

        for p in pieces:
            if p.isnumeric():
                address.append(p)
            else:
                address.append(' '.join(re.split('(\d+)',' '.join(re.findall('[^A-Z]*[A-Z][^A-Z]*',p)))))

        return ', '.join(address)



    def toLocalDate(self, utcdate):                
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/Mexico_City')
        utc = utcdate                 
        utc = utc.replace(tzinfo=from_zone)
        central = utc.astimezone(to_zone)
        return str(central)
    
        
    def get_due_service(self, w, word1, word2, word3, flag = False):

        words = word1.split(',')

        inx1 = -1
        for word in words:
            inx1 = w.find(word)
            if inx1 > 0:
                word1 = word
                break

        if inx1 < 0:
            return None

        if flag:     
            w = w.replace(word1, word1 + word2)

        inx2 = w.find(word2, inx1 + (len(word1) - 1))
        inx3 = w.find(word3, inx2 + (len(word2)))

        due = w[inx2:inx3 + (len(word3))]
        
        mapping = [" ", word2, word3]

        for k in mapping:
            due = due.replace(k, "")

        return due
