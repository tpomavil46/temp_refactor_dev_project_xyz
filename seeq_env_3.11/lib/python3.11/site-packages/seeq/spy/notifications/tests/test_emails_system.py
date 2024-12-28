import mock
import pytest

from seeq import spy
from seeq.spy import Status
from seeq.spy._errors import SPyValueError, SPyRuntimeError
from seeq.spy.notifications._emails import EmailRecipient, EmailAttachment, EmailRequestInput
from seeq.spy.tests import test_common


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_send_email():
    with mock.patch('seeq.spy.notifications._emails._call_send_email_api', mock.Mock(return_value=None)):
        test_status1 = Status()
        spy.notifications.send_email('test@seeq.com', 'Hello', '<p>HTML content</p>', status=test_status1)
        assert 'Your message has been sent' in test_status1.message
        assert test_status1.code == Status.SUCCESS

        test_status2 = Status()
        spy.notifications.send_email(
            to=[
                'test@seeq.com',
                EmailRecipient(name='Test Name', email='test.email@seeq.com')
            ],
            cc="some_cc_user@seeq.com",
            bcc=['bcc.recipient@seeq.com', 'another.bcc@seeq.com'],
            subject='Subject',
            content='Email content',
            attachments=EmailAttachment(content='gibberish', type='application/pdf', filename='test.pdf'),
            status=test_status2)
        assert 'Your message has been sent' in test_status2.message
        assert test_status2.code == Status.SUCCESS

        test_status3 = Status(quiet=True)
        spy.notifications.send_email('test@seeq.com', 'Subject', 'Content', status=test_status3)
        assert test_status3.code == Status.SUCCESS

    recipient1 = EmailRecipient('test@seeq.com')
    attachment1 = EmailAttachment('content1', 'application/pdf', 'test1.pdf')

    def validate_args_one_attachment(*args):
        assert len(args) == 2
        assert args[1] == EmailRequestInput(toEmails=[recipient1], subject='subject', content='content',
                                            attachments=[attachment1])

    with mock.patch('seeq.spy.notifications._emails._call_send_email_api', mock.Mock(
            side_effect=validate_args_one_attachment)):
        spy.notifications.send_email(
            to=recipient1,
            subject='subject',
            content='content',
            attachments=attachment1
        )

    attachment2 = EmailAttachment('content2', 'image/png', 'test2.png')

    def validate_args_two_attachments(*args):
        assert len(args) == 2
        assert args[1] == EmailRequestInput(toEmails=[recipient1], subject='subject', content='content',
                                            attachments=[attachment1, attachment2])

    with mock.patch('seeq.spy.notifications._emails._call_send_email_api', mock.Mock(
            side_effect=validate_args_two_attachments)):
        spy.notifications.send_email(
            to=recipient1,
            subject='subject',
            content='content',
            attachments=[attachment1, attachment2]
        )

    error_reason = "Your message could not be sent."
    with mock.patch('seeq.spy.notifications._emails._call_send_email_api',
                    mock.Mock(side_effect=SPyRuntimeError(error_reason))):
        with pytest.raises(SPyRuntimeError, match=error_reason):
            spy.notifications.send_email(to='test@seeq.com', subject='Subject', content='Content')

        test_status1 = Status(errors='catalog')
        spy.notifications.send_email('test@seeq.com', 'Subject', 'Content', status=test_status1)
        assert test_status1.code == Status.FAILURE
        assert error_reason in test_status1.message

        test_status2 = Status(quiet=True, errors='catalog')
        spy.notifications.send_email('test@seeq.com', 'Subject', 'Content', status=test_status2)
        assert test_status2.code == Status.FAILURE

    with pytest.raises(SPyValueError, match='At least one recipient needs to be provided'):
        spy.notifications.send_email(to=[], subject='Subject', content='Content')

    with pytest.raises(SPyValueError, match='A non blank subject must be provided'):
        spy.notifications.send_email(to='test@seeq.com', subject='', content='Content')

    with pytest.raises(SPyValueError, match='A non blank content must be provided'):
        spy.notifications.send_email(to='test@seeq.com', subject='Subject', content='')


@pytest.mark.skip(reason="CRAB-36082 For manual testing. This only works if the email configuration has "
                         "actually been set up for the target Seeq instance.")
@pytest.mark.system
def test_send_real_email():
    spy.login(url='https://monitors.seeq.dev', access_key='', password='')
    spy.session.options.allow_version_mismatch = True
    spy.notifications.send_email(
        to=['test@seeq.com', EmailRecipient(name='Test Name', email='test.email@seeq.com')],
        cc="some_cc_user@seeq.com",
        bcc=['bcc.recipient@seeq.com'],
        subject='SPy System Test: test_send_real_email',
        content='Email content')
