# def trading_status(request):
#     user = getattr(request, 'user', None)
#     trading_enabled = False
#     if user and user.is_authenticated:
#         trading_enabled = getattr(user, 'trading_enabled', False)
#     return {'trading_enabled': trading_enabled}


def trading_status(request):
    user = getattr(request, 'user', None)
    trading_enabled = False
    if user and user.is_authenticated:
        trading_enabled = getattr(user, 'trading_enabled', False)
    return {'trading_enabled': trading_enabled}