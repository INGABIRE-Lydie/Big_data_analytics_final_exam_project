import json
import datetime as dt
import happybase

HBASE_HOST = 'localhost'   # adjust if needed
MAX_TS = 9999999999999     # for reverse timestamp

def iso_to_epoch_millis(iso_str):
    dt_obj = dt.datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return int(dt_obj.timestamp() * 1000)

def make_row_key(user_id, start_time_iso):
    ts = iso_to_epoch_millis(start_time_iso)
    rev_ts = MAX_TS - ts
    return f"{user_id}#{rev_ts:013d}"  # zero-padded for correct ordering

def main():
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table('ecom:user_sessions')  # IMPORTANT: use ecom: namespace

    count = 0
    max_rows = 50000  # representative subset

    with open('sessions.json', 'r', encoding='utf-8') as f:
        for line in f:
            if count >= max_rows:
                break

            s = json.loads(line)
            row_key = make_row_key(s['user_id'], s['start_time'])

            device = s.get('device_profile', {})
            geo = s.get('geo_data', {})

            page_views = s.get('page_views', [])
            page_count = len(page_views)
            product_detail_views = sum(
                1 for pv in page_views if pv.get('page_type') == 'product_detail'
            )

            data = {
                b's:start_time':        s['start_time'].encode('utf-8'),
                b's:end_time':          s['end_time'].encode('utf-8'),
                b's:duration_seconds':  str(s['duration_seconds']).encode('utf-8'),
                b's:conversion_status': s['conversion_status'].encode('utf-8'),
                b's:referrer':          s['referrer'].encode('utf-8'),
                b's:device_type':       device.get('type', '').encode('utf-8'),
                b's:os':                device.get('os', '').encode('utf-8'),
                b's:browser':           device.get('browser', '').encode('utf-8'),
                b's:city':              geo.get('city', '').encode('utf-8'),
                b's:state':             geo.get('state', '').encode('utf-8'),
                b's:country':           geo.get('country', '').encode('utf-8'),
                b's:ip_address':        geo.get('ip_address', '').encode('utf-8'),

                b'pv:page_count':          str(page_count).encode('utf-8'),
                b'pv:product_detail_views': str(product_detail_views).encode('utf-8'),
            }

            table.put(row_key, data)
            count += 1

    print(f"Loaded {count} sessions into HBase")

if __name__ == "__main__":
    main()