import base64
import datetime
import urllib.parse
import urllib.request
import settings_and_imports
import json
import imaplib
import smtplib, ssl
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

settings = settings_and_imports.Settings


def incrementaOra(ora):
    return ora + 1


def url_escape(text):
    return urllib.parse.quote(text, safe='~-._')


def url_unescape(text):
    return urllib.parse.unquote(text)


def url_format_params(params):
    param_fragments = []
    for param in sorted(params.items(), key=lambda x: x[0]):
        param_fragments.append('%s=%s' % (param[0], url_escape(param[1])))
    return '&'.join(param_fragments)


def call_authorize_tokens(client_id, client_secret, authorization_code):
    params = {'client_id': client_id, 'client_secret': client_secret, 'code': authorization_code,
              'redirect_uri': settings.REDIRECT_URI, 'grant_type': 'authorization_code'}
    request_url = command_to_url('o/oauth2/token')
    response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
    return json.loads(response)


def call_refresh_token(client_id, client_secret, refresh_token):
    params = {'client_id': client_id, 'client_secret': client_secret, 'refresh_token': refresh_token,
              'grant_type': 'refresh_token'}
    request_url = command_to_url('o/oauth2/token')
    response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
    return json.loads(response)


def command_to_url(command):
    return '%s/%s' % (settings.GOOGLE_ACCOUNTS_BASE_URL, command)


def generate_permission_url(client_id, scope='https://mail.google.com/'):
    params = {'client_id': client_id, 'redirect_uri': settings.REDIRECT_URI, 'scope': scope, 'response_type': 'code'}
    return '%s?%s' % (command_to_url('o/oauth2/auth'), url_format_params(params))


def generate_oauth2_string(username, access_token, as_base64=False):
    auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
    if as_base64:
        auth_string = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
    return auth_string


def test_imap(user, auth_string):
    imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
    imap_conn.debug = 4
    imap_conn.authenticate('XOAUTH2', lambda x: auth_string)
    imap_conn.select('INBOX')


def test_smpt(user, base64_auth_string):
    smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_conn.set_debuglevel(True)
    smtp_conn.ehlo('test')
    smtp_conn.starttls()
    smtp_conn.docmd('AUTH', 'XOAUTH2 ' + base64_auth_string)


def get_authorization():
    scope = "https://mail.google.com/"
    print('Navigate to the following URL to auth:', generate_permission_url(settings.GOOGLE_CLIENT_ID, scope))
    authorization_code = input('Enter verification code: ')
    response = call_authorize_tokens(settings.GOOGLE_CLIENT_ID, settings.GOOGLE_CLIENT_SECRET, authorization_code)
    return response['refresh_token'], response['access_token'], response['expires_in']


def refresh_authorization(refresh_token):
    response = call_refresh_token(settings.GOOGLE_CLIENT_ID, settings.GOOGLE_CLIENT_SECRET, refresh_token)
    return response['access_token'], response['expires_in']


def send_mail(fromaddr, toaddr, subject, message):
    access_token, expires_in = refresh_authorization(settings.GOOGLE_REFRESH_TOKEN)
    auth_string = generate_oauth2_string(fromaddr, access_token, as_base64=True)
    '''
    msg = MIMEMultipart('related')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg.preamble = 'This is a multi-part message in MIME format.'
    msg_alternative = MIMEMultipart('alternative')
    msg.attach(msg_alternative)
    part_text = MIMEText(lxml.html.fromstring(message).text_content().encode('utf-8'), 'plain', _charset='utf-8')
    part_html = MIMEText(message.encode('utf-8'), 'html', _charset='utf-8')
    msg_alternative.attach(part_text)
    msg_alternative.attach(part_html)
    '''
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo(settings.GOOGLE_CLIENT_ID)
    server.starttls()
    server.docmd('AUTH', 'XOAUTH2 ' + auth_string)
    server.sendmail(fromaddr, toaddr.split(', '), "To: " + toaddr + "\nFrom: " + fromaddr + "\nSubject: " +
                    subject + "\n" + message)
    # server.sendmail(fromaddr, toaddr, msg.as_string())
    server.quit()


def invia_email(mittente, lista_destinatari, oggetto, testo):
    if settings.GOOGLE_REFRESH_TOKEN is None:
        print('No refresh token found, obtaining one')
        refresh_token, access_token, expires_in = get_authorization()
        print('Set the following as your GOOGLE_REFRESH_TOKEN:', refresh_token)
        exit()
    lista_dest = ", ".join(lista_destinatari)

    send_mail(mittente, lista_dest, oggetto, testo)


def get_month_number_to_string(int_mese):
    if int_mese == 1:
        return 'January'
    if int_mese == 2:
        return 'February'
    if int_mese == 3:
        return 'March'
    if int_mese == 4:
        return 'April'
    if int_mese == 5:
        return 'May'
    if int_mese == 6:
        return 'June'
    if int_mese == 7:
        return 'July'
    if int_mese == 8:
        return 'August'
    if int_mese == 9:
        return 'September'
    if int_mese == 10:
        return 'October'
    if int_mese == 11:
        return 'November'
    if int_mese == 12:
        return 'December'


def get_formatted_path_day_number(int_day):
    string_day_formatted = '0'
    if int_day < 10:
        string_day_formatted = string_day_formatted + str(int_day)
    else:
        string_day_formatted = str(int_day)
    return string_day_formatted


def genera_path_soluzione():
    string_solution_file_pt_uno = "Y:\\Plexos\\Archivio simulazioni\\Dispacciamento Servola\\Storico\\"
    string_solution_file_pt_due = "\\Project con e senza vincoli Solution\\Project con e senza vincoli Solution.zip"
    oggi = datetime.date.today()
    string_full_path = string_solution_file_pt_uno + str(oggi.year) + '\\' + get_month_number_to_string(oggi.month) + '\\' + get_formatted_path_day_number(oggi.day) + '\\14' + string_solution_file_pt_due
    return string_full_path


def send_mail_ez(strPyScript, strMailServer, strMailPort, strMailUser, strMailPWD, strMailFrom, lstMailTo,
                  strMailSubject, strMailBody):
        try:
            smtpObj = smtplib.SMTP(strMailServer, strMailPort)
            smtpObj.ehlo()
            smtpObj.starttls()
            smtpObj.ehlo()
            smtpObj.login(strMailUser, strMailPWD)
            strMailCode = 'To: %s\nFrom: %s\nSubject: %s\n\n%s' % (
            (', '.join(lstMailTo)), strMailFrom,
            strMailSubject, strMailBody)
            smtpObj.sendmail(strMailFrom, lstMailTo, strMailCode.encode('utf-8'))
            smtpObj.quit()
        except Exception as exc:
            print('%s Error: Unable to send email - %s - %s' % (time.strftime("%H:%M:%S"), strPyScript, str(exc)))


def send_html_mail_ez(strMailServer, intMailPort, strMailUser, strMailPWD, strMailFrom, lstMailTo,
                  strMailSubject, strMailBody):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = strMailSubject
    msg["From"] = strMailFrom
    msg["To"] = ''.join(lstMailTo)
    part = MIMEText(strMailBody, "html")
    msg.attach(part)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(strMailServer, intMailPort, context=context) as server:
        server.login(strMailUser, strMailPWD)
        server.sendmail(
            strMailFrom, lstMailTo, msg.as_string()
        )
