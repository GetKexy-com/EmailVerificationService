import { IsEmail, IsString, MinLength } from 'class-validator';

export class RegisterDto {
  @IsString({ message: 'First name is required' })
  first_name: string;

  @IsString({ message: 'Last name is required' })
  last_name: string;

  @IsString({ message: 'Timezone is required' })
  timezone: string;

  @IsString({ message: 'Email is required' })
  @IsEmail({}, { message: 'Email is invalid' })
  email_address: string;

  @IsString({ message: 'Password is required' })
  @MinLength(8)
  password: string;
}
