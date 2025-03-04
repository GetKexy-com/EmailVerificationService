import { Body, Controller, Get, Post, Req, UseGuards } from '@nestjs/common';

import { AuthService } from '@/auth/auth.service';
import { RegisterDto } from '@/auth/dto/register.dto';
import { JwtAuthGuard } from '@/auth/guards/jwt.guard';
import { LocalGuard } from '@/auth/guards/local.guard';
import { Public } from '@/common/decorators/public.decorator';

@Controller('auth')
export class AuthController {
  constructor(private authService: AuthService) {}

  @Post('login')
  @Public()
  @UseGuards(LocalGuard)
  login(@Req() req: any) {
    return { auth_token: req.user };
  }

  @Post('register')
  @Public()
  register(@Body() registerDto: RegisterDto) {
    return this.authService.register(registerDto);
  }

  @Get('status')
  @UseGuards(JwtAuthGuard)
  status(@Req() req: any) {
    return req.user;
  }
}
