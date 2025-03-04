import { IsNotEmpty, IsString } from 'class-validator';

export class CreateEmailRoleDto {
  @IsString()
  @IsNotEmpty()
  readonly role: string;
}
