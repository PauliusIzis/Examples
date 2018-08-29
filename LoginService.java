package main.Services.Impl;

import main.Models.BL.UserModel;
import main.Models.CONS.Settings;
import main.Models.DAL.UserDAL;
import main.Models.DTO.LoginDTO;
import main.Models.DTO.UserDTO;
import main.Services.ICache;
import main.Services.IHigherService;
import main.Services.ILoginService;

import javax.servlet.http.Cookie;
import java.util.UUID;

public class LoginService implements ILoginService {

    private ICache cache = Cache.getInstance();

    @Override
    public LoginDTO find(String email, String password) {
        try {
            IHigherService hs = new HigherService();
            UserDTO userDTO = hs.getUserByEmail(email);
            String hash = userDTO.user.password;
            boolean testHash = HashService.check(password, hash);
            if (!testHash) {
                return new LoginDTO(true, "Incorrect password", null);
            }
            return new LoginDTO(true, "", addUserToCache(userDTO.user));
        } catch (Exception e) {
            e.printStackTrace();
            return new LoginDTO(false, "Connection error", null);
        }
    }

    private UserModel addUserToCache(UserDAL userDAL) {
        String uuid = UUID.randomUUID().toString();
        UserModel user = new UserModel(userDAL.userName, userDAL.userId, uuid);
        cache.put(uuid, user);
        return user;
    }

    @Override
    public boolean isValid(Cookie cookie) {

        if (cookie == null || cache.get(cookie.getValue()) == null) {
            return false;
        }

        return true;
    }

    @Override
    public Cookie findTokenCookie(Cookie[] cookies) {
        if (cookies != null && cookies.length > 0) {
            for (Cookie ck : cookies) {
                if (ck.getName().equals(Settings.COOKIE_NAME)) {
                    return ck;
                }
            }
        }
        return null;
    }
}
