from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from ai_sales_radar.channel_posts import (
    ChannelPostError,
    MAX_IMAGE_BYTES,
    MAX_MEDIA_CAPTION_LENGTH,
    MAX_TEXT_LENGTH,
    _image_path,
    _post_text,
    _text_file,
)


class ChannelPostInputTest(unittest.TestCase):
    def test_plain_post_accepts_trimmed_text(self):
        self.assertEqual(_post_text("  Готовый пост  ", has_image=False), "Готовый пост")

    def test_empty_post_is_rejected(self):
        with self.assertRaisesRegex(ChannelPostError, "пуст"):
            _post_text("  \n ", has_image=False)

    def test_text_and_caption_use_separate_limits(self):
        self.assertEqual(
            len(_post_text("x" * MAX_TEXT_LENGTH, has_image=False)),
            MAX_TEXT_LENGTH,
        )
        with self.assertRaisesRegex(ChannelPostError, str(MAX_MEDIA_CAPTION_LENGTH)):
            _post_text("x" * (MAX_MEDIA_CAPTION_LENGTH + 1), has_image=True)

    def test_text_file_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "post.txt"
            target.write_text("Пост", encoding="utf-8")
            link = root / "post-link.txt"
            link.symlink_to(target)

            with self.assertRaisesRegex(ChannelPostError, "обычным файлом"):
                _text_file(link)

    def test_image_rejects_symlink_and_unsupported_suffix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "preview.png"
            target.write_bytes(b"png")
            link = root / "preview-link.png"
            link.symlink_to(target)

            with self.assertRaisesRegex(ChannelPostError, "обычным файлом"):
                _image_path(link)

            unsupported = root / "preview.svg"
            unsupported.write_text("<svg/>", encoding="utf-8")
            with self.assertRaisesRegex(ChannelPostError, "JPG, PNG и WEBP"):
                _image_path(unsupported)

    def test_image_size_limit_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "large.png"
            image.write_bytes(b"x" * (MAX_IMAGE_BYTES + 1))

            with self.assertRaisesRegex(ChannelPostError, "10 МБ"):
                _image_path(image)


if __name__ == "__main__":
    unittest.main()
