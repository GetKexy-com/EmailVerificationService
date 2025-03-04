import { IsString, IsArray, IsObject, IsOptional, IsNotEmpty, IsNumber } from 'class-validator';

class MailHeader {
  @IsString()
  name: string;

  @IsString()
  value: string;
}

class MailCommonHeaders {
  @IsArray()
  @IsString({ each: true })
  from: string[];

  @IsArray()
  @IsString({ each: true })
  to: string[];

  @IsString()
  subject: string;
}

class MailTags {
  @IsArray()
  @IsString({ each: true })
  configurationSet: string[];
}

class Mail {
  @IsString()
  timestamp: string;

  @IsString()
  source: string;

  @IsArray()
  @IsString({ each: true })
  destination: string[];

  @IsString()
  messageId: string;

  @IsString()
  sendingAccountId: string;

  @IsArray()
  @IsOptional()
  headers: MailHeader[];

  @IsObject()
  commonHeaders: MailCommonHeaders;

  @IsObject()
  tags: MailTags;
}

class Delivery {
  @IsString()
  timestamp: string;

  @IsNumber()
  processingTimeMillis: number;

  @IsArray()
  @IsString({ each: true })
  recipients: string[];

  @IsString()
  smtpResponse: string;

  @IsString()
  reportingMTA: string;
}

class BouncedRecipient {
  @IsString()
  emailAddress: string;

  @IsString()
  action: string;

  @IsString()
  status: string;

  @IsString()
  diagnosticCode: string;
}

class Bounce {
  @IsString()
  bounceType: string;

  @IsString()
  bounceSubType: string;

  @IsArray()
  bouncedRecipients: BouncedRecipient[];

  @IsString()
  timestamp: string;

  @IsString()
  feedbackId: string;
}

class ComplaintRecipient {
  @IsString()
  emailAddress: string;
}

class Complaint {
  @IsArray()
  complainedRecipients: ComplaintRecipient[];

  @IsString()
  timestamp: string;

  @IsString()
  feedbackId: string;

  @IsString()
  complaintFeedbackType: string;

  @IsString()
  userAgent: string;
}

class DelayedRecipients {
  @IsString()
  emailAddress: string;

  @IsString()
  status: string;

  @IsString()
  diagnosticCode: string;
}

class DeliveryDelay {
  @IsString()
  timestamp: string;

  @IsNumber()
  delayTimeMillis: number;

  @IsArray()
  @IsString({ each: true })
  delayedRecipients: DelayedRecipients[];

  @IsString()
  reason: string;
}

export class SnsNotificationDto {
  @IsString()
  @IsNotEmpty()
  eventType: string;

  @IsObject()
  mail: Mail;

  @IsObject()
  @IsOptional()
  delivery?: Delivery;

  @IsObject()
  @IsOptional()
  bounce?: Bounce;

  @IsObject()
  @IsOptional()
  complaint?: Complaint;

  @IsObject()
  @IsOptional()
  deliveryDelay?: DeliveryDelay;
}