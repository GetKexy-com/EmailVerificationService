import * as dns from 'dns';
import { EventEmitter } from 'events';
import * as net from 'net';

const LIMIT = 200;

type DNSBLResult = {
  status: string;
  A?: string;
  TXT?: string;
  address?: string;
};

const dnsblList = [
  {
    zone: 'zen.spamhaus.org',
  },
  {
    zone: 'bl.spamcop.net',
  },
  {
    zone: 'dnsbl.sorbs.net',
  },
  {
    zone: 'b.barracudacentral.org',
  },
];

function expandIPv6Address(address: string): string {
  let fullAddress = '';
  let expandedAddress = '';
  const validGroupCount = 8;
  const validGroupSize = 4;

  const extractIpv4 = /([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})/;
  const validateIpv4 =
    /((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})/;

  if (validateIpv4.test(address)) {
    const groups = address.match(extractIpv4);
    if (groups) {
      let ipv4 = '';
      for (let i = 1; i < groups.length; i++) {
        ipv4 +=
          ('00' + parseInt(groups[i], 10).toString(16)).slice(-2) +
          (i === 2 ? ':' : '');
      }
      address = address.replace(extractIpv4, ipv4);
    }
  }

  if (!address.includes('::')) {
    fullAddress = address;
  } else {
    const sides = address.split('::');
    const groupsPresent = sides.reduce(
      (acc, side) => acc + side.split(':').length,
      0,
    );
    fullAddress =
      sides[0] +
      ':' +
      '0000:'.repeat(validGroupCount - groupsPresent) +
      sides[1];
  }

  const groups = fullAddress.split(':');
  for (let i = 0; i < validGroupCount; i++) {
    while (groups[i].length < validGroupSize) {
      groups[i] = '0' + groups[i];
    }
    expandedAddress += i !== validGroupCount - 1 ? groups[i] + ':' : groups[i];
  }
  return expandedAddress;
}

function reverseIP(address: string): string {
  if (net.isIPv4(address)) {
    return address.split('.').reverse().join('.');
  } else if (net.isIPv6(address)) {
    const expanded = expandIPv6Address(address);
    return expanded.split(/:|/).reverse().join('.');
  }
  return address;
}

function doALookup(
  host: string,
  callback: (err: Error | null, res?: DNSBLResult) => void,
): void {
  dns.resolve(host, (err, addresses) => {
    if (err) {
      if (err.code === 'ENOTFOUND') {
        return callback(null, { status: 'not_listed' });
      } else {
        return callback(err);
      }
    }
    dns.resolveTxt(host, (err, records) => {
      if (err) return callback(err);
      callback(null, {
        status: 'listed',
        A: addresses.join(', '),
        TXT: records?.join('\n'),
      });
    });
  });
}

function multiLookup(
  addresses: string | string[],
  list: any[],
  limit: number = LIMIT,
): void {
  addresses = Array.isArray(addresses) ? addresses : [addresses];
  const root = this;

  addresses.forEach((address) => {
    const lookupAddress = reverseIP(address);
    list.forEach((item) => {
      const zone = item.zone || item;
      const host = `${lookupAddress}.${zone}`;
      doALookup(host, (err, res) => {
        if (err) root.emit('error', err, item);
        else {
          res!.address = address;
          root.emit('data', res, item);
        }
      });
    });
  });
  root.emit('done');
}

class DNSBL extends EventEmitter {
  constructor(ipOrDomain: string, list?: any[], limit: number = LIMIT) {
    super();
    if (net.isIPv4(ipOrDomain)) {
      list = list || dnsblList;
      multiLookup.call(this, ipOrDomain, list, limit);
    } else {
      dns.resolve(ipOrDomain, (err, addresses) => {
        if (err) {
          this.emit('error', err);
          this.emit('done');
        } else if (addresses) {
          list = list || dnsblList;
          multiLookup.call(this, addresses, list, limit);
        }
      });
    }
  }
}

export { DNSBL, reverseIP };
