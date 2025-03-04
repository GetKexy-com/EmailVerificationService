import { Body, Controller, Post, Req, UnauthorizedException, UseGuards } from '@nestjs/common';
import { minutes, Throttle } from '@nestjs/throttler';

import { JwtAuthGuard } from '@/auth/guards/jwt.guard';
import { EmailDto } from '@/domains/dto/email.dto';
import { DomainService } from '@/domains/services/domain.service';
import { User } from '@/users/entities/user.entity';
import { UsersService } from '@/users/users.service';

@Controller('email')
export class DomainsController {
  constructor(
    private readonly domainService: DomainService,
    private readonly userService: UsersService,
  ) {
  }

  @Throttle({
    default: { limit: 50000, ttl: minutes(1), blockDuration: minutes(1) },
  })
  @Post('validate')
  @UseGuards(JwtAuthGuard)
  async validate(@Req() req: any, @Body() emailDto: EmailDto) {
    const { email } = emailDto;
    // const user = req.user.user_id;
    const user: User = await this.userService.findOneById(req.user.id);
    if (!user) {
      // this.winstonLoggerService.error('Domain Controller validate()', `No user found for user_id: ${firstPendingFile.user_id}`);

      return new UnauthorizedException('Session expired');
    }

    return await this.domainService.smtpValidation(email, user);
  }
}
