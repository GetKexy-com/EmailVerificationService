export const LEAD_WRAP = 'Lead Wrap';
export const MX_RECORD_CHECK_DAY_GAP = 365;
export const SPAM_DB_CHECK_DAY_GAP = 100;
export const CATCH_ALL_CHECK_DAY_GAP = 100;
export const ERROR_DOMAIN_CHECK_DAY_GAP = 0;
export const PROCESSED_EMAIL_CHECK_DAY_GAP = 0;

// In queue service expected time is in milliseconds. So we calculate it for 15 min
export const GREY_LIST_MINUTE_GAP = 2 * 60 * 1000;
export const SMTP_RESPONSE_MAX_DELAY = 3000;
export const PROCESS_EMAIL_SEND_QUEUE = 'sendEmail';
export const QUEUE = 'emailVerificationQueue';
export const PROCESS_GREY_LIST_QUEUE = 'processGreyListQueue';
export const PROCESS_BULK_FILE_QUEUE = 'processBulkFileQueue';
export const DEV = 'development';
export const PROD = 'production';

export enum MicrosoftDomains {
  HOTMAIL = 'hotmail.com',
  OUTLOOK = 'outlook.com',
  LIVE = 'live.com',
  MSN = 'msn.com',
}