package main.Services.Impl;

import main.Models.BL.UserModel;
import main.Models.DAL.UserDAL;
import main.Models.DAL.UserExtendedDAL;
import main.Models.DTO.DBqueryDTO;
import main.Models.DTO.RegisterDTO;
import main.Models.DTO.UserDTO;
import main.Services.ICache;
import main.Services.IHigherService;
import main.Services.IRegisterService;
import org.modelmapper.ModelMapper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

public class RegisterService implements IRegisterService {

    IHigherService hs = new HigherService();
    ICache cache = Cache.getInstance();

    @Override
    public RegisterDTO find(String userName, String email) {
        UserDTO userDTO = hs.getUserByUserNameAndEmail(userName, email);

        if (userDTO.success) {
            return new RegisterDTO(true, null, null);
        } else {
        	//Review. Model mapper should be used trough instances and probably as service
            ModelMapper mod = new ModelMapper();
            mod.getConfiguration().setFieldMatchingEnabled(true);
            return new RegisterDTO(false, null, null);
        }

    }

    @Override
    public RegisterDTO register(String userName, String password, String email) throws IOException {

        String hash = null;
        try {
            hash = HashService.getSaltedHash(password);
        } catch (Exception e) {
        	//Review. Log exception
            e.printStackTrace();
        }

        UserDAL user = new UserDAL();
        user.userName = userName;
        user.password = hash;
        user.email = email;
        DBqueryDTO dto = hs.registerUser(user);

        if (!dto.success) {
            return new RegisterDTO(false, dto.message, null);
        }

        UserDTO userDTO = hs.getUserByEmail(email);
        //Review. Too many dto as names
        if (!userDTO.success) {
            return new RegisterDTO(false, userDTO.message, null);
        }
        UserExtendedDAL userExtendedDAL = getUserExtendedDAL(userDTO.user);
        dto = hs.insertNewUserExtended(userExtendedDAL);
        //Review. 1) !dto.success same if twice 2) if basic user reg is successful and
        //extented not successful both are not successful. Is this ok?
        //3) If first registers are inserted, no revert exist
        if (!dto.success) {
            return new RegisterDTO(false, dto.message, null);
        }

        return new RegisterDTO(true, "", null);
    }

    private UserExtendedDAL getUserExtendedDAL(UserDAL userDAL) throws IOException {
        UserExtendedDAL userExtendedDAL = new UserExtendedDAL();
        userExtendedDAL.userId = userDAL.userId;
        userExtendedDAL.userName = userDAL.userName;
        userExtendedDAL.draw = 0;
        userExtendedDAL.lose = 0;
        userExtendedDAL.win = 0;
        userExtendedDAL.totalFights = 0;
        //Review. Never add unconrolled data as "=" 
        //Byte[] arr = getImgByteArray();
        //if(arr.length != 0) userExtendedDAL.profileImg = arr;
        userExtendedDAL.profileImg = getImgByteArray();
        return userExtendedDAL;
    }

    private byte[] getImgByteArray() throws IOException {
    	//Review. This can brake. Control it.
        BufferedImage bufferedImage = ImageIO.read(getClass().getResource("/main/webapp/Images/imageLeft.png"));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "png", byteArrayOutputStream);
        return byteArrayOutputStream.toByteArray();
    }


    public UserModel addUserToCache(String email) {
        UserDTO userDTO = hs.getUserByEmail(email);
        if (!userDTO.success) {
        	//Review. LOL
            //TODO Return not null
            return null;
        }
        String uuid = UUID.randomUUID().toString();
        UserModel user = new UserModel(userDTO.user.userName, userDTO.user.userId, uuid);
        cache.put(uuid, user);
        return user;
    }

}