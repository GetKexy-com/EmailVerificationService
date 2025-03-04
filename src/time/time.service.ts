import { Injectable } from '@nestjs/common';
import { differenceInDays, differenceInMinutes } from 'date-fns';
import { PROCESSED_EMAIL_CHECK_DAY_GAP } from '@/common/utility/constant';
import { ProcessedEmail } from '@/domains/entities/processed_email.entity';
import { EmailStatus } from '@/common/utility/email-status-type';

@Injectable()
export class TimeService {
  public getTimeDifferenceInMin(date1: Date, date2: Date) {
    return differenceInMinutes(date1, date2);
  }

  public getTimeDifferenceInDays(date1: Date, date2: Date) {
    return differenceInDays(date1, date2);
  }

  public shouldReturnCachedProcessedEmail(processedEmail: ProcessedEmail): boolean {
    const dayPassedSinceLastMxCheck = this.getTimeDifferenceInDays(
      new Date(),
      processedEmail.created_at,
    );

    // If processed email has one of the below email_status, then we will revalidate the
    // email and not use database response.
    if (
      (
        dayPassedSinceLastMxCheck < PROCESSED_EMAIL_CHECK_DAY_GAP
      )
      &&
      (
        processedEmail.email_status === EmailStatus.VALID ||
        processedEmail.email_status === EmailStatus.CATCH_ALL ||
        processedEmail.email_status === EmailStatus.SPAMTRAP ||
        processedEmail.email_status === EmailStatus.DO_NOT_MAIL
      )
    ) {
      return true;
    }
  }

}
