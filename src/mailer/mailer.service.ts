import { Injectable } from '@nestjs/common';
import * as nodemailer from 'nodemailer';
import * as process from 'node:process';
import * as hbs from 'nodemailer-express-handlebars';
import { ConfigService } from '@nestjs/config';
import { randomStringGenerator } from '@nestjs/common/utils/random-string-generator.util';
import { SendMailOptions } from '@/common/utility/email-status-type';
import { DEV } from '@/common/utility/constant';

@Injectable()
export class MailerService {
  private transporter: nodemailer.Transporter;

  constructor(private readonly configService: ConfigService) {
    this.transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: parseInt(process.env.SMTP_PORT),
      secure: false, // true for 465, false for other ports
      from: process.env.SMTP_FROM_NAME,
      auth: {
        user: process.env.SMTP_USERNAME, // Your email
        pass: process.env.SMTP_PASSWORD, // Your email password or app password
      },
    });

    let templatePath: string;
    if (configService.get<string>('NODE_ENV') === DEV) {
      templatePath = `${process.cwd()}/src/mailer/templates`;
    } else {
      templatePath = `${process.cwd()}/mailer/templates`;
    }

    // Configure Handlebars
    this.transporter.use(
      'compile',
      hbs({
        viewEngine: {
          extname: '.hbs',
          layoutsDir: templatePath,
          defaultLayout: false,
        },
        viewPath: templatePath,
        extName: '.hbs',
      }),
    );
  }

  async sendEmail(mailData: SendMailOptions) {
    const {
      fromEmail = '',
      to,
      bcc = [],
      subject,
      template = '',
      attachments = [],
      headers = {},
      context = {},
    } = mailData;
    const mailOptions = {
      from: fromEmail || this.configService.get<string>('SMTP_FROM_EMAIL'),
      to,
      bcc,
      subject,
      template, // Handlebars template name (without .hbs)
      context, // Dynamic data for template
      attachments,
      headers,
    };

    try {
      const info = await this.transporter.sendMail(mailOptions);
      console.log('Email sent: ', info.messageId);
      return info;
    } catch (error) {
      console.error('Error sending email:', error);
      throw error;
    }
  }
}
