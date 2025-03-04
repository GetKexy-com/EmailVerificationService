import { Injectable, UnauthorizedException } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { Strategy } from 'passport-local';

import { AuthService } from '../auth.service';

@Injectable()
export class LocalStrategy extends PassportStrategy(Strategy) {
  constructor(private authService: AuthService) {
    super({
      usernameField: 'email_address',
    });
  }

  validate(email_address: string, password: string) {
    console.log(email_address);
    const user = this.authService.validateUser({ email_address, password });
    if (!user)
      throw new UnauthorizedException('Username or password is incorrect');
    return user;
  }
}
