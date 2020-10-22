local cjson = require("cjson")
local jwt = require "resty.jwt"
local base64 = require "base64"
local b64_url =  require("ngx.base64")
local pkey = require'openssl'.pkey
local bn = require'openssl'.bn

local REQUIRED_ENV_VARS = {"LIVY_URL", "KEYSTORE_DATA"}
for index, value in ipairs(REQUIRED_ENV_VARS) do
    if not os.getenv(value) then error("Missing env variable: " .. value) end
end

local JWKs = cjson.decode(base64.decode(os.getenv("KEYSTORE_DATA"))).keys

function respond_401(log_message)
    ngx.header.content_type = "text/plain"
    ngx.status = 401
    ngx.say("401 Unauthorized")
    if log_message then ngx.log(ngx.WARN, log_message) end
    return ngx.exit(401)
end

function get_jwk(kid)
    for index, value in ipairs(JWKs) do
        if value.kid == kid then
            return value
        end
    end
    return nil
end

function jwk_to_pem(jwk)
    local rsa_public_key = pkey.new({alg = 'rsa', n = bn.text(b64_url.decode_base64url(jwk.n)), e = bn.text(b64_url.decode_base64url(jwk.e))})
    return rsa_public_key:export('pem')
end

function check_jwt(token)
    if not token then respond_401("Missing token") end
    
    local jwt_obj = jwt:load_jwt(token)
    if not jwt_obj.valid then respond_401("Invalid token: " .. jwt_obj.reason) end
    
    local jwk = get_jwk(jwt_obj.header.kid)
    if not jwk then respond_401("No JWK found that matches token kid " .. jwt_obj.header.kid) end

    local jwt_verified = jwt:verify_jwt_obj(jwk_to_pem(jwk), jwt_obj)
    if not jwt_verified.verified then
        respond_401("Verify JWT failed: " .. jwt_verified.reason)
    else
        return jwt_verified
    end
end

function substitute_proxy_user(jwt_object)
    ngx.req.read_body()
    local raw_body = ngx.req.get_body_data()

    if not raw_body or not pcall(cjson.decode, raw_body) then return end

    local body = cjson.decode(raw_body)
    
    if body and body.proxyUser then  
        local new_body = body
        
        local sub_suffix = jwt_object.payload.sub:sub(1,3)
        local username_base = (jwt_object.payload["preferred_username"]
            and jwt_object.payload["preferred_username"]
            or jwt_object.payload["cognito:username"])

        new_body.proxyUser = username_base .. sub_suffix
        ngx.req.set_body_data(cjson.encode(new_body))
    end
end

function remove_token_query_param()
    local args = ngx.req.get_uri_args()
    args.token = nil
    ngx.req.set_uri_args(args)
end

