from http.cookiejar import DefaultCookiePolicy
from urllib.request import Request


class BlockAllCookiesPolicy(DefaultCookiePolicy):
    def set_ok(self, cookie, request):
        return False

    def return_ok(self, cookie, request):
        return False

    def domain_return_ok(self, domain, request):
        return False

    def path_return_ok(self, path, request):
        return False