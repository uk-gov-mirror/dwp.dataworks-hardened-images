local jwt = check_jwt(ngx.var.arg_token)
substitute_proxy_user(jwt)
remove_token_query_param()
