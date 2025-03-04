import { Injectable } from '@nestjs/common';
import { SnsNotificationDto } from '@/webhook/dto/sns-notification.dto';
import { DomainService } from '@/domains/services/domain.service';
import {
  EmailReason,
  EmailStatus,
  EmailValidationResponseType,
} from '@/common/utility/email-status-type';
import { RetryStatus } from '@/domains/entities/processed_email.entity';

@Injectable()
export class WebhookService {
  constructor(
    private domainService: DomainService,
  ) {
  }

  public async handleSnsNotification(notification: SnsNotificationDto) {
    let email: string = '';
    let emailStatus: EmailValidationResponseType;
    switch (notification.eventType) {
      case 'Send':
        break;
      case 'Delivery':
        email = notification.delivery.recipients[0];
        if(notification.delivery.smtpResponse.includes('queued')) {
          return notification.delivery;
        }
        emailStatus = {
          email_address: email,
          verify_plus: true,
          email_status: EmailStatus.VALID,
          email_sub_status: EmailReason.EMPTY,
          retry: RetryStatus.COMPLETE,
        };
        await this.domainService.updateProcessedEmailByEmail(email, emailStatus);

        return notification.delivery;
      case 'DeliveryDelay':
        email = notification.deliveryDelay.delayedRecipients[0].emailAddress;
        emailStatus = {
          email_address: email,
          verify_plus: true,
          email_status: EmailStatus.UNKNOWN,
          email_sub_status: EmailReason.GREY_LISTED,
          retry: RetryStatus.PENDING,
        };
        await this.domainService.updateProcessedEmailByEmail(email, emailStatus);

        return notification.deliveryDelay;
      case 'Bounce':
        email = notification.bounce.bouncedRecipients[0].emailAddress;
        emailStatus = {
          email_address: email,
          verify_plus: true,
          email_status: EmailStatus.INVALID,
          email_sub_status: EmailReason.MAILBOX_NOT_FOUND,
          retry: RetryStatus.COMPLETE,
        };
        await this.domainService.updateProcessedEmailByEmail(email, emailStatus);

        break;
      case 'Complaint':
        break;

    }

    return { status: 'Notification Processed' };
  }
}
