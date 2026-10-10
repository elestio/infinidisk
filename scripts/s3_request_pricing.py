#!/usr/bin/env python3
"""Classify observed S3 HTTP requests and project published Tigris request fees.

This is benchmark accounting, not a provider billing statement. No credentials,
object names or query values are returned by the classifier. Count each upstream
attempt once; do not collapse SDK retries into one logical operation.
"""
import argparse
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

ROOT = Path(__file__).resolve().parents[1]
PRICING_FILE = ROOT / 'validation/astra/s3-operations/pricing-tigris-2026-10-10.json'

# Explicit API names in the published price list, restricted to the operations
# relevant to these storage engines. Unlisted APIs must not silently become B.
EXPLICIT_A = frozenset({
    'CreateBucket', 'CreateMultipartUpload', 'CopyObject', 'ListObjects',
    'ListObjectsV2', 'ListMultipartUploads', 'ListBuckets', 'ListParts',
    'PutObject', 'PutObjectAcl', 'PutObjectTagging', 'PutObjectRetention',
    'PutObjectLegalHold', 'PutObjectLockConfiguration', 'PutBucketAcl',
    'PutBucketPolicy', 'PutBucketCors', 'PutBucketLifecycleConfiguration',
    'PutBucketTagging', 'PutBucketAccelerateConfiguration',
    'PutBucketOwnershipControls',
})
EXPLICIT_B = frozenset({
    'GetObject', 'HeadObject', 'HeadBucket', 'GetObjectAcl', 'GetObjectTagging',
    'GetBucketLocation', 'GetBucketAcl', 'GetBucketCors',
    'GetBucketLifecycleConfiguration', 'GetBucketPolicy', 'GetBucketPolicyStatus',
    'GetBucketTagging', 'GetBucketVersioning', 'GetBucketRequestPayment',
    'GetBucketAccelerateConfiguration', 'GetBucketOwnershipControls',
})
FREE = frozenset({'DeleteObject', 'DeleteObjects', 'DeleteBucket',
                  'AbortMultipartUpload', 'DeleteObjectTagging',
                  'DeleteBucketCors', 'DeleteBucketLifecycle',
                  'DeleteBucketPolicy', 'DeleteBucketTagging'})
# These are PUT/POST operations but are not individually named in that price
# list. Keep the assumption visible even when their estimated rate is class A.
ASSUMED_A = frozenset({'UploadPart', 'UploadPartCopy', 'CompleteMultipartUpload'})


def operation_class(operation):
    if operation in EXPLICIT_A:
        return 'A', 'explicit_api'
    if operation in EXPLICIT_B:
        return 'B', 'explicit_api'
    if operation in FREE or operation.startswith('Delete'):
        return 'FREE', 'delete_or_cancel'
    if operation in ASSUMED_A:
        return 'A', 'assumed_put_post_multipart'
    return 'UNKNOWN', 'not_in_published_mapping'


def classify_s3_request(method, raw_path, headers=None):
    """Classify a path-style S3 request, using raw inputs only in memory.

    The result intentionally has no path, key, header or query-value field.
    The proxy records its own phase, upstream attempt and response status.
    """
    method = method.upper()
    parsed = urlsplit(raw_path)
    parts = parsed.path.lstrip('/').split('/', 1)
    bucket = bool(parts[0])
    obj = len(parts) == 2 and bool(parts[1])
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    lower_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    copy = 'x-amz-copy-source' in lower_headers
    operation = 'Unknown'
    if method == 'DELETE':
        operation = ('AbortMultipartUpload' if 'uploadId' in query else
                     'DeleteObject' if obj else 'DeleteBucket')
    elif method == 'POST' and 'delete' in query:
        operation = 'DeleteObjects'
    elif method == 'POST' and obj and 'uploads' in query:
        operation = 'CreateMultipartUpload'
    elif method == 'POST' and obj and 'uploadId' in query:
        operation = 'CompleteMultipartUpload'
    elif method == 'PUT' and obj and 'uploadId' in query and 'partNumber' in query:
        operation = 'UploadPartCopy' if copy else 'UploadPart'
    elif method == 'GET' and 'uploadId' in query:
        operation = 'ListParts'
    elif method == 'GET' and 'uploads' in query:
        operation = 'ListMultipartUploads'
    else:
        subresource = next((key for key in (
            'acl', 'tagging', 'cors', 'lifecycle', 'location', 'policy',
            'policyStatus', 'versioning', 'requestPayment', 'accelerate',
            'ownershipControls', 'retention', 'legal-hold', 'object-lock',
        ) if key in query), None)
        suffix = {'acl': 'Acl', 'tagging': 'Tagging', 'cors': 'Cors',
                  'lifecycle': 'LifecycleConfiguration', 'location': 'Location',
                  'policy': 'Policy', 'policyStatus': 'PolicyStatus',
                  'versioning': 'Versioning', 'requestPayment': 'RequestPayment',
                  'accelerate': 'AccelerateConfiguration',
                  'ownershipControls': 'OwnershipControls', 'retention': 'Retention',
                  'legal-hold': 'LegalHold', 'object-lock': 'LockConfiguration'}
        if subresource and method in ('GET', 'PUT'):
            operation = ('Get' if method == 'GET' else 'Put') + ('Object' if obj else 'Bucket') + suffix[subresource]
        elif method == 'HEAD':
            operation = 'HeadObject' if obj else 'HeadBucket'
        elif method == 'GET':
            operation = ('GetObject' if obj else 'ListBuckets' if not bucket else
                         'ListObjectsV2' if query.get('list-type') == '2' else
                         'ListObjectVersions' if 'versions' in query else 'ListObjects')
        elif method == 'PUT':
            operation = ('CopyObject' if copy else 'PutObject') if obj else 'CreateBucket'
    request_class, basis = operation_class(operation)
    return {'operation': operation, 'request_class': request_class,
            'classification_basis': basis, 'scope': 'object' if obj else 'bucket'}


def request_cost(class_a, class_b, *, apply_monthly_free_tier=False, pricing=None):
    """USD for these counts; monthly allowance is account-wide, never per test."""
    if type(class_a) is not int or type(class_b) is not int or min(class_a, class_b) < 0:
        raise ValueError('request counts must be nonnegative integers')
    pricing = pricing or json.loads(PRICING_FILE.read_text())
    a, b = class_a, class_b
    if apply_monthly_free_tier:
        a = max(0, a - pricing['monthly_free_requests']['A'])
        b = max(0, b - pricing['monthly_free_requests']['B'])
    dollars = (Decimal(a) * Decimal(str(pricing['usd_per_million_requests']['A']))
               + Decimal(b) * Decimal(str(pricing['usd_per_million_requests']['B']))) / Decimal(1_000_000)
    return float(dollars)


def project_events(events, transactions=None, *, pricing=None):
    """Aggregate redacted proxy events, preserving uncertain/unpriced requests.

    Each event needs operation (or api), status, and forwarded. A missing
    forwarded flag is accepted for completed upstream response events only.
    A network failure after dispatch has an uncertain provider receipt/bill.
    """
    pricing = pricing or json.loads(PRICING_FILE.read_text())
    exempt = set(pricing['explicitly_nonbillable_http_statuses'])
    classes, charged, operations, statuses, phases, kinds = (Counter() for _ in range(6))
    local_only = exempt_count = uncertain = assumed = unknown = total = 0
    payload_sent = payload_received = 0
    seen_ids = set()
    for event in events:
        if 'id' in event:
            identity = event['id']
            if identity in seen_ids:
                raise ValueError('duplicate request id: use one consolidated record per attempt')
            seen_ids.add(identity)
        status = event.get('status')
        forwarded = event.get('forwarded', type(status) is int)
        if not forwarded:
            local_only += 1
            continue
        total += 1
        op = event.get('operation', event.get('api', 'Unknown'))
        cls, basis = operation_class(op)
        classes[cls] += 1
        operations[op] += 1
        statuses[str(status) if type(status) is int else 'no_response'] += 1
        phases[str(event.get('phase', 'unlabelled'))] += 1
        kinds[str(event.get('object_type', 'unclassified'))] += 1
        payload_sent += event.get('request_bytes', event.get('upload_bytes', 0)) or 0
        payload_received += event.get('response_bytes', event.get('download_bytes', 0)) or 0
        assumed += basis.startswith('assumed_')
        unknown += cls == 'UNKNOWN'
        if type(status) is not int or event.get('transport_error'):
            uncertain += 1
            continue
        if status in exempt:
            exempt_count += 1
            continue
        if cls in ('A', 'B'):
            charged[cls] += 1
    if transactions is not None and (type(transactions) is not int or transactions <= 0):
        raise ValueError('normalization requires a positive completed transaction count')
    cost = request_cost(charged['A'], charged['B'], pricing=pricing)
    return {
        'total_upstream_attempts': total, 'local_only_requests': local_only,
        'request_classes': {key: classes[key] for key in ('A', 'B', 'FREE', 'UNKNOWN')},
        'estimated_billable_requests': {key: charged[key] for key in ('A', 'B')},
        'explicitly_nonbillable_status_requests': exempt_count,
        'transport_uncertain_requests': uncertain, 'unknown_class_requests': unknown,
        'assumed_class_requests': assumed,
        'by_operation': dict(sorted(operations.items())), 'by_status': dict(sorted(statuses.items())),
        'by_phase': dict(sorted(phases.items())), 'by_object_type': dict(sorted(kinds.items())),
        'request_payload_bytes': payload_sent, 'response_payload_bytes': payload_received,
        'estimated_gross_request_cost_usd': cost,
        'completed_transactions_for_normalization': transactions,
        'estimated_usd_per_1000_transactions': cost * 1000 / transactions if transactions else None,
        'estimated_usd_per_10000_transactions': cost * 10000 / transactions if transactions else None,
        'upstream_attempts_per_million_transactions': total * 1_000_000 / transactions if transactions else None,
        'estimated_usd_per_million_transactions': cost * 1_000_000 / transactions if transactions else None,
        'cost_has_unpriced_or_uncertain_requests': bool(unknown or uncertain),
        'pricing_source': pricing['source_url'], 'pricing_verified_utc': pricing['verified_utc'],
        'monthly_free_tier_applied': False,
        'scope': 'Request fees only, projected from observed Elestio API attempts; not a Tigris invoice. Retry attempts are not deduplicated. Unknown classes and transport failures are retained outside the priced subtotal. Multipart class A is explicitly an assumption when not named in the price list. Storage, retrieval, notifications, taxes and negotiated rates are excluded.',
    }


def sql_query_units(summary, sql_accounting):
    """Normalize an observed batch; this does not forecast a sustained workload."""
    transactions = summary['completed_transactions_for_normalization']
    queries = sql_accounting['business_queries']
    if not transactions or queries != transactions * sql_accounting['queries_per_transaction']:
        raise ValueError('SQL query/transaction denominator mismatch')
    return {
        'completed_business_queries': queries,
        'all_commands_including_begin_end': sql_accounting['all_commands_including_begin_end'],
        'queries_per_transaction': sql_accounting['queries_per_transaction'],
        'basis': sql_accounting['basis'],
        'estimated_usd_per_1000_queries': summary['estimated_gross_request_cost_usd'] * 1000 / queries,
        'estimated_usd_per_10000_queries': summary['estimated_gross_request_cost_usd'] * 10000 / queries,
        'class_a_per_1000_queries': summary['request_classes']['A'] * 1000 / queries,
        'class_b_per_1000_queries': summary['request_classes']['B'] * 1000 / queries,
        'normalization_is_forecast': False,
    }


def self_test():
    import unittest

    class AccountingTests(unittest.TestCase):
        def test_rest_and_safe_output(self):
            cases = [('GET', '/bucket/prefix/HEAD', {}, 'GetObject', 'B'),
                     ('HEAD', '/bucket/prefix/x', {}, 'HeadObject', 'B'),
                     ('GET', '/bucket?list-type=2&prefix=secret', {}, 'ListObjectsV2', 'A'),
                     ('GET', '/bucket?uploads=', {}, 'ListMultipartUploads', 'A'),
                     ('GET', '/bucket/x?uploadId=secret', {}, 'ListParts', 'A'),
                     ('PUT', '/bucket/x', {}, 'PutObject', 'A'),
                     ('PUT', '/bucket/x', {'X-Amz-Copy-Source': 'secret'}, 'CopyObject', 'A'),
                     ('POST', '/bucket/x?uploads=', {}, 'CreateMultipartUpload', 'A'),
                     ('PUT', '/bucket/x?uploadId=secret&partNumber=1', {}, 'UploadPart', 'A'),
                     ('POST', '/bucket/x?uploadId=secret', {}, 'CompleteMultipartUpload', 'A'),
                     ('DELETE', '/bucket/x?uploadId=secret', {}, 'AbortMultipartUpload', 'FREE'),
                     ('POST', '/bucket?delete=', {}, 'DeleteObjects', 'FREE'),
                     ('GET', '/bucket?versions=', {}, 'ListObjectVersions', 'UNKNOWN')]
            for method, path, headers, operation, cls in cases:
                with self.subTest(operation=operation):
                    result = classify_s3_request(method, path, headers)
                    self.assertEqual((result['operation'], result['request_class']), (operation, cls))
                    self.assertNotIn('secret', json.dumps(result))

        def test_attempts_exempt_status_and_uncertainty(self):
            events = [dict(operation=op, status=status, forwarded=True) for op, status in (
                ('PutObject', 200), ('PutObject', 200), ('PutObject', 412),
                ('GetObject', 206), ('HeadObject', 404), ('DeleteObjects', 200),
                ('UploadPart', 200), ('Unknown', 200), ('GetObject', None))]
            result = project_events(events, transactions=10)
            self.assertEqual(result['total_upstream_attempts'], 9)
            self.assertEqual(result['request_classes'], {'A': 4, 'B': 3, 'FREE': 1, 'UNKNOWN': 1})
            self.assertEqual(result['estimated_billable_requests'], {'A': 3, 'B': 2})
            self.assertEqual(result['explicitly_nonbillable_status_requests'], 1)
            self.assertEqual(result['transport_uncertain_requests'], 1)
            self.assertEqual(result['assumed_class_requests'], 1)
            self.assertEqual(result['estimated_gross_request_cost_usd'], .000016)
            self.assertEqual(result['estimated_usd_per_million_transactions'], 1.6)
            self.assertTrue(result['cost_has_unpriced_or_uncertain_requests'])

        def test_prices_and_global_allowances(self):
            self.assertEqual(request_cost(1_000_000, 1_000_000), 5.5)
            self.assertEqual(request_cost(10_000, 100_000, apply_monthly_free_tier=True), 0)
            self.assertEqual(request_cost(10_001, 100_001, apply_monthly_free_tier=True), .0000055)
            with self.assertRaises(ValueError):
                request_cost(-1, 1)
            with self.assertRaises(ValueError):
                project_events([], transactions=0)
            with self.assertRaises(ValueError):
                project_events([{'id': 1}, {'id': 1}])

    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(AccountingTests))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--events', type=Path)
    parser.add_argument('--transactions', type=int)
    args = parser.parse_args()
    if args.self_test:
        self_test()
    elif args.events:
        with args.events.open() as stream:
            print(json.dumps(project_events((json.loads(line) for line in stream if line.strip()), args.transactions), indent=2))
    else:
        parser.error('choose --self-test or --events')
