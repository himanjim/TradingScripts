from multiprocessing import Pool
import face_recognition
import time, os

image_dir = 'E:/RJIL_POS Crop Images Dec 2019/'


def work_log(img):
    face_recognition.face_encodings (face_recognition.load_image_file (image_dir + img))


pool_size = 5
max_images_to_encode = 50
images = os.listdir(image_dir)


def pool_handler():
    p = Pool(pool_size)
    p.map(work_log, images[0:max_images_to_encode])


if __name__ == '__main__':
    start_time = time.time ()
    pool_handler()
    print ("---%d images encoded in %s seconds ---" % (max_images_to_encode, time.time () - start_time))