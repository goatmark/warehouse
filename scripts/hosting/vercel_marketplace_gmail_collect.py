"""Read explicit Supabase marketplace invoice lines from Vercel receipts.

No recurring charge is projected. Installation ambiguity, mixed invoice totals
or duplicate billing periods fail before loading new monetary facts.
"""
import argparse
import base64
from datetime import datetime,timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import urllib.parse
import urllib.request
from supabase_gmail_fetch import credentials_from_local
from vercel_costs import credential,TEAMS

INSTALLATION='icfg_pbsLC2qEGvpFG5qG84juGA7W'
MONTH=r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'


def parse_receipt(text,message_id,at):
    if '[Supabase] Pro Plan' not in text or 'Vercel Inc' not in text:return None
    price=re.search(r'\[Supabase\] Pro Plan\s*[-–]\s*\$([\d,]+\.\d{2}) per unit\s+Qty\s+(\d+)',text)
    paid=re.search(r'Amount paid\s+\$([\d,]+\.\d{2})',text)
    period=re.search(rf'({MONTH})\s+(\d{{1,2}})\W+({MONTH})\s+(\d{{1,2}}),?\s+(\d{{4}})',text)
    if not price or not paid or not period:raise ValueError('Explicit marketplace invoice format requires review: '+message_id)
    total=Decimal(paid.group(1).replace(',',''))
    if total!=Decimal(price.group(1).replace(',',''))*int(price.group(2)):
        raise ValueError('Marketplace invoice has mixed charges/adjustments; do not assign whole total')
    sm,sd,em,ed,year=period.groups();year=int(year)
    start=datetime.strptime(f'{sm} {sd} {year}','%b %d %Y').replace(tzinfo=timezone.utc)
    end=datetime.strptime(f'{em} {ed} {year}','%b %d %Y').replace(tzinfo=timezone.utc)
    if start>=end:start=start.replace(year=year-1)
    invoice=re.search(r'Invoice number\s+([A-Z0-9]+-\d+)',text)
    receipt=re.search(r'Receipt\s*#\s*([\d-]+)',text)
    if not invoice and not receipt:raise ValueError('Native invoice/receipt identity missing')
    identity=invoice.group(1) if invoice else 'receipt-'+receipt.group(1)
    payment=re.search(r'Paid (?:on )?([A-Z][a-z]+ \d{1,2}, \d{4})',text)
    paid_on=datetime.strptime(payment.group(1),'%B %d, %Y').date().isoformat() if payment else at[:10]
    return {'vendor':'supabase','billing_vendor':'vercel','resource_id':'marketplace:'+INSTALLATION,
        'resource_name':'website / explicit Supabase Pro Plan invoice','billing_service':'Supabase Pro Plan',
        'period_start':start.isoformat(),'period_end':end.isoformat(),'amount_usd':str(total),'currency':'USD',
        'cost_kind':'account_fee','source_kind':'invoice','source_id':identity+':supabase-pro',
        'evidence_url':'https://mail.google.com/mail/u/0/#all/'+message_id,
        'snapshot_at':datetime.now(timezone.utc).isoformat(),'paid_on':paid_on}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True);p.add_argument('--ownership-output',required=True)
    a=p.parse_args()
    team,env,auth=TEAMS['goatmarks-projects'];token=credential(env,auth)
    def vercel(path):
        with urllib.request.urlopen(urllib.request.Request('https://api.vercel.com'+path,headers={'Authorization':'Bearer '+token}),timeout=45) as r:return json.load(r)
    listing=vercel('/v1/integrations/configurations?'+urllib.parse.urlencode({'teamId':team,'view':'account'}))
    configs=listing if isinstance(listing,list) else listing.get('configurations',[])
    matches=[x for x in configs if x.get('slug')=='supabase' and x.get('installationType')=='marketplace']
    if len(matches)!=1 or matches[0]['id']!=INSTALLATION:raise ValueError('Supabase installation ownership is ambiguous or changed')
    detail=matches[0]
    if 'website' not in json.dumps(detail.get('vendorBillingPlan',{})).lower():raise ValueError('Current native installation no longer proves website ownership')
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    raw=os.environ.get('HOSTING_GMAIL_OAUTH_JSON')
    auth=json.loads(raw) if raw else credentials_from_local(Path.home()/'.gmail-mcp/credentials.json',Path.home()/'.gmail-mcp/token-markkhouryme.json')
    c=Credentials(token=None,refresh_token=auth['refresh_token'],client_id=auth['client_id'],client_secret=auth['client_secret'],token_uri=auth['token_uri']);c.refresh(Request())
    def gmail(path):
        with urllib.request.urlopen(urllib.request.Request('https://gmail.googleapis.com/gmail/v1/users/me/'+path,headers={'Authorization':'Bearer '+c.token}),timeout=45) as r:return json.load(r)
    if gmail('profile')['emailAddress'].lower()!='mark@markkhoury.me':raise ValueError('Marketplace invoice mailbox identity mismatch')
    page=None;rows={};periods={}
    while True:
        params={'q':'"[Supabase]" "Pro Plan" after:2026/01/01','maxResults':100}
        if page:params['pageToken']=page
        listing=gmail('messages?'+urllib.parse.urlencode(params))
        for item in listing.get('messages',[]):
            msg=gmail('messages/'+item['id']+'?format=full');parts=[];stack=[msg['payload']]
            while stack:
                part=stack.pop();stack.extend(part.get('parts',[]));data=part.get('body',{}).get('data')
                if data and part.get('mimeType')=='text/plain':parts.append(base64.urlsafe_b64decode(data+'='*(-len(data)%4)).decode('utf-8','replace'))
            at=datetime.fromtimestamp(int(msg['internalDate'])/1000,timezone.utc).isoformat()
            row=parse_receipt('\n'.join(parts),item['id'],at)
            if not row:continue
            period=(row['period_start'],row['period_end'])
            if period in periods and periods[period]!=row['source_id']:raise ValueError('Multiple native invoice identities for one marketplace period; reconcile before loading')
            periods[period]=row['source_id'];rows[row['source_id']]=row
        page=listing.get('nextPageToken')
        if not page:break
    Path(a.output).write_text(''.join(json.dumps(r)+'\n' for r in rows.values()),encoding='utf-8')
    ownership=[{'vendor':'supabase','resource_id':'marketplace:'+INSTALLATION,'project':'website','service':'database','environment':'mixed','valid_from':'2026-06-16','valid_to':None,
        'evidence_url':'https://api.vercel.com/v1/integrations/configurations?view=account&teamId='+team}]
    Path(a.ownership_output).write_text(json.dumps(ownership),encoding='utf-8')
    print('Validated',len(rows),'explicit native marketplace invoices against unique live installation')


if __name__=='__main__':main()
