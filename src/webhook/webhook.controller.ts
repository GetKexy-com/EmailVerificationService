import { BadRequestException, Body, Controller, Post, Req } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { Public } from '@/common/decorators/public.decorator';
import { SnsNotificationDto } from '@/webhook/dto/sns-notification.dto';
import { validate } from 'class-validator';
import { WinstonLoggerService } from '@/logger/winston-logger.service';
import { minutes, Throttle } from '@nestjs/throttler';

@Controller('webhook')
export class WebhookController {
  constructor(
    private readonly webhookService: WebhookService,
    private readonly winstonLoggerService: WinstonLoggerService,
  ) {
  }

  @Public()
  @Throttle({
    default: { limit: 50000, ttl: minutes(1), blockDuration: minutes(1) },
  })
  @Post('sns-delivery-status')
  async receiveEmailDeliveryStatus(
    @Req() req: any,
    @Body() body: any,
  ) {
    if (!req.secure) {
      throw new BadRequestException('HTTPS required');
    }
    const headers = req.headers;
    const messageType = headers['x-amz-sns-message-type'];
    if (messageType === 'SubscriptionConfirmation') {
      return { status: 'Subscription Confirmed' };
    }

    // Validate the incoming payload
    const notification = new SnsNotificationDto();
    Object.assign(notification, body);

    const errors = await validate(notification);
    if (errors.length > 0) {
      // Log error in error log to debug
      const logData = {
        errors,
        payload: body,
      };
      this.winstonLoggerService.error('SNS-Delivery-Error', logData.toString());
      return;
    }

    return await this.webhookService.handleSnsNotification(notification);
  }
}
