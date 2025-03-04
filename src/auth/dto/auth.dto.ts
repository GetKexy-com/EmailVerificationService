import { IsEmail, IsString } from 'class-validator';

export class AuthPayloadDto {
  @IsEmail()
  @IsString()
  email_address: string;

  @IsString()
  password: string;
}
