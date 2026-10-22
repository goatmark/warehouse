"""Fetch Supabase invoice PDF attachments with read-only Gmail API operations."""
import argparse
import base64
import io
import json
import os
import re
import urllib.parse
import urllib.request
import unicodedata
from pathlib import Path


def credentials_from_local(client_file, token_file):
    client = json.loads(Path(client_file).read_text(encoding='utf-8'))
    client = client.get('installed') or client.get('web') or client
    token = json.loads(Path(token_file).read_text(encoding='utf-8'))
    return {'client_id': client['client_id'], 'client_secret': client['client_secret'],
            'refresh_token': token['refresh_token'], 'token_uri': 'https://oauth2.googleapis.com/token'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--oauth-file')
    parser.add_argument('--client-file', default=str(Path.home() / '.gmail-mcp/credentials.json'))
    parser.add_argument('--token-file', default=str(Path.home() / '.gmail-mcp/token-markkhouryme.json'))
    parser.add_argument('--query', default='Supabase has:attachment filename:pdf after:2026/01/01')
    args = parser.parse_args()
    raw = os.environ.get('HOSTING_GMAIL_OAUTH_JSON')
    auth = json.loads(raw) if raw else json.loads(Path(args.oauth_file).read_text()) if args.oauth_file else credentials_from_local(args.client_file, args.token_file)
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from pypdf import PdfReader
    credentials = Credentials(token=None, refresh_token=auth['refresh_token'], client_id=auth['client_id'],
                              client_secret=auth['client_secret'], token_uri=auth.get('token_uri', 'https://oauth2.googleapis.com/token'))
    credentials.refresh(Request())
    def get(path):
        request = urllib.request.Request('https://gmail.googleapis.com/gmail/v1/users/me/' + path,
            headers={'Authorization': 'Bearer ' + credentials.token})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    profile = get('profile')
    if profile['emailAddress'].lower() != 'mark@markkhoury.me':
        raise RuntimeError('Invoice mailbox identity mismatch; no attachments downloaded')
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    page, downloaded, seen = None, 0, []
    while True:
        query = {'q': args.query, 'maxResults': 100}
        if page:
            query['pageToken'] = page
        result = get('messages?' + urllib.parse.urlencode(query))
        for message in result.get('messages', []):
            full = get(f"messages/{message['id']}?format=full")
            stack = [full['payload']]
            while stack:
                part = stack.pop()
                stack.extend(part.get('parts', []))
                filename = part.get('filename', '')
                attachment = part.get('body', {}).get('attachmentId')
                if not filename.lower().endswith('.pdf') or not attachment:
                    continue
                blob = get(f"messages/{message['id']}/attachments/{attachment}")
                content = base64.urlsafe_b64decode(blob['data'] + '=' * (-len(blob['data']) % 4))
                text = '\n'.join(p.extract_text() for p in PdfReader(io.BytesIO(content)).pages)
                text = ''.join(c for c in text if unicodedata.category(c) != 'Cf')
                if not re.search(r'^Supabase\s+(?:Pte|Inc|LLC)', text[:350], re.M | re.I):
                    continue
                text = text.replace('\x00', '-')
                invoice_id = re.search(r'\b[A-Z0-9]{4,20}\s*-\s*\d{4,8}\b', text)
                if not invoice_id:
                    if 'Invoice number' in text:
                        label = re.search(r'Invoice number\s+([^\n]+)', text)
                        raise RuntimeError('Supabase invoice PDF identifier format requires review: ' + (label.group(1)[:60] if label else 'missing'))
                    continue
                invoice_id = re.sub(r'\s+', '', invoice_id.group(0))
                file = output / f'Invoice-{invoice_id}.pdf'
                if not file.exists():
                    file.write_bytes(content)
                    downloaded += 1
                seen.append({'invoice_id': invoice_id, 'gmail_message_id': message['id'],
                             'evidence_url': f"https://mail.google.com/mail/u/0/#all/{message['id']}"})
        page = result.get('nextPageToken')
        if not page:
            break
    (output / 'gmail-invoice-evidence.json').write_text(json.dumps(seen, indent=2), encoding='utf-8')
    print(json.dumps({'provider': 'supabase', 'mailbox': profile['emailAddress'],
                      'invoice_attachments_seen': len(seen), 'new_pdf_files': downloaded}))


if __name__ == '__main__':
    main()
